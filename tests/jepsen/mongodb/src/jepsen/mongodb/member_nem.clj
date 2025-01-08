(ns jepsen.mongodb.member-nem
    "A nemesis about adding and removing members from replica set"
    (:require   [clojure.tools.logging :refer :all]
                [clojure.set :as cset]
                [jepsen [nemesis :as jnem]
                        [control :as jcontrol]
                        [db :as db]]
                [jepsen.generator :as jgen]
                [jepsen.mongodb [db :as mdb]
                                [client :as mcl]]))


;; use a set to record the removed nodes
(def crashing-status (
  ref #{}
  ;; :validator pos? ;; there might need a validator
)) 

(def version-cnt (
  ref 1 
  :validator pos? ;; there might need a validator
))

(def member-id-cnt (
  ref 0
  :validate (partial <= 0)
))


(defn grace-remove-cleanup
  "The kill didn't follow the remove procedure defined by mongodb, so follow the remaining step here before adding new members
  This function should emulate an admin maintaining process (i.e. has identified all failing nodes and performing updating)
  "
  [test, replica-set-db, nodes, removed]
  ;; According to Mongodb, it allows adding or removing no more than 1 voting member at a time. So perform one by one
  (let
    [
      port (if (mdb/config-server? test) mcl/config-port mcl/shard-port)
      rm-seq (seq removed)
      ;; live-members (cset/difference (set nodes) removed)
    ]
    (doall
      (for [n rm-seq]
        (let
          [
            cur_prim (->>
              (cset/difference (set nodes) removed) ;; calculate remaining alive nodes
              (assoc test :nodes) ;; for following code, we only need to check alive nodes
              (db/primaries replica-set-db)
              (first));; get only the first result
            old-version (deref version-cnt)
            new-version (+ old-version 1)
          ]
          (info "in grace remove, node " n ". current primary is " cur_prim " port is " port)
          (with-open [ conn (mcl/open cur_prim port) ]
            (let
              [
                old-config (:config (mcl/admin-command! conn { :replSetGetConfig 1 }))
                id (:_id old-config)
                ;; Get the member list by removing 1 member
                new-members (vec (filter #(not= (mcl/addr->node (:host %)) n) (:members old-config)))
                ;; Construct new config
                new-config {:_id id, :version new-version, :members new-members}
              ]
              (mcl/admin-command! conn { :replSetReconfig new-config })
              ;; Update version cnt
              (dosync (ref-set version-cnt new-version))
              (info "cleaned up for " n)
            )
          )
        )
      )
    )
  )
)


(defn reconfig-for-adding
  "Add one member by reconfigurate the replica-set
  "
  [test, replica-set-db, primary, port, new-member]

  (with-open [ conn (mcl/open primary port) ] ;; open a connect to the primary node
    (let
      [
        old-config (:config (mcl/admin-command! conn { :replSetGetConfig 1 })) ;; get current configuration
        id (:_id old-config)
        old-version (deref version-cnt)
        new-version (+ old-version 1)
        old-member-list (:members old-config)
        ;; Each replica set member must have a unique _id. Avoid re-using _id values even if no members[n] entry is using that _id in the current configuration.
        new-member-id (+ (deref member-id-cnt) 1)
        new-member-list (->>
          ;; construct a new member document
          ;; For now only add voting members and doesn't consider hidden problem, for simplicity set priority be 1
          {:_id new-member-id, :votes 1, :host (str new-member ":" port), :priority 1, :hidden false} 
          ;; add with old member list
          (conj (vec old-member-list))
        )
        ;; Construct new config
        new-config {:_id id, :version new-version, :members new-member-list}
      ]
      (info "New member list is: ", new-member-list)
      ;; (try
        (mcl/admin-command! conn { :replSetReconfig new-config })
      ;;   (catch Exception e (info "Reconfig should have completed, the error is\n" e) nil)
      ;; )
      ;; update version-cnt and member-id-cnt
      (dosync (ref-set version-cnt new-version))
      (dosync (ref-set member-id-cnt new-member-id))
      (info "Added a new member: " new-member "via reconfiguration")
    )
  )
)


(defn add-with-reconfig
  "Add members 1 by 1 to the replica set via re-configuration.
  This function should emulate an admin maintaining process (i.e. has identified all failing nodes and performing updating)
  "
  [test, replica-set-db, nodes, targets]
  ;; Assume 
  ;; 1. /etc/mongod.conf on nodes doesn't need to change, stay as db setup
  ;; 2. the data directory should have been wiped out, hence MongoDB will use its initial syncing feature to restore the data.

  ;; Start the mongod
  (jcontrol/on-nodes test targets mdb/start!)
  ;; Get current config
  (doall ;; enforce evaluation
    (for [n targets]
      (let
        [
          live-members (cset/difference (set nodes) targets), ;; members that are alive before add
          live-x (first live-members)
          port (if (mdb/config-server? test) mcl/config-port mcl/shard-port)
        ]
        ;; On an alive members, get the current primary from the current status of the replicaset 
        (with-open [ live-conn (mcl/open live-x port) ]
          (let [cur-prim (mdb/primary live-conn)] ;; this command should be able to run on any alive members
            (info "In add-with-reconfig, Current primary is: " cur-prim)
            ;; perform reconfig
            (reconfig-for-adding test replica-set-db cur-prim port n)
          )
        )
      )
    )
  )
)


(defn force-reconfig
  "When there are less than 3 nodes available in the cluster, force a reconfig"
  [test, replica-set-db, nodes, removed]
  (let
    [
      live-members (cset/difference (set nodes) removed), ;; members that are alive before add
      live-x (first live-members),
      port (if (mdb/config-server? test) mcl/config-port mcl/shard-port)
    ]
    ;; 1. the kill didn't follow the remove procedure defined by mongodb, but now has minority of primary
    ;; 2 Make sure the new member's data directory does not contain data
    (jcontrol/on-nodes test removed mdb/wipe!)
    ;; 3 Start the mongod for all
    (jcontrol/on-nodes test removed mdb/start!)
    (info "In FORCE, restarted mongod")
    ;; 4. Force a reconfig
    (with-open [ live-conn (mcl/open live-x port) ]
      (let
        [
          old-config (:config (mcl/admin-command! live-conn { :replSetGetConfig 1 })) ;; get the current configuration
        ]
        (info "The recovered config via FORCE is:\n" old-config)
        ;; enforce a reconfig
        ;; according to MongoDB, the configuration is then propagated to all the surviving members listed in the members array. The replica set will then
        ;; elects a new primary.
        ;; Don't need to update the config, otherwise the "force" command would complain
        (mcl/admin-command! live-conn { :replSetReconfig old-config, :force true })
        (info "End of FORCE reconfig. Added " removed "via force reconfiguration.")
      )
    )
  )
)


(defn add-members
  "Add voting members into the replica set.
  As required by MongoDB, assume add even numbers of members.
  "
  [test]
  ;; (info "joining the replica set" )
  (let
    [
      removed (deref crashing-status) ;; removed count should always be even
      removed-cnt (count removed)
      nodes (:nodes test)
      remaining-cnt (- (count nodes) removed-cnt)
      replica-set-db (:db test)
    ]

    (if (>= remaining-cnt 3)
      ;; if the replica set has more than 3 members, can just add with reconfig
      (let
        [
          target (set removed)
        ]
        ;; Now add new members step by step
        ;; 1. the kill didn't follow the remove procedure defined by mongodb, so follow here
        (grace-remove-cleanup test replica-set-db nodes removed)
        ;; 2. now (re)-add new members
        ;; 2.1 Make sure the new member's data directory does not contain data
        (jcontrol/on-nodes test removed mdb/wipe!)
        (info "Should have removed old data")
        ;; 2.2 Add the new member into the replica set
        (add-with-reconfig test replica-set-db nodes target)

        ;; update status
        (dosync (ref-set crashing-status (cset/difference removed (set target))))
        (info "Add member " target " new crashing status is " (deref crashing-status))
      )
      ;; Otherwise, should have no primary now, need to force a reconfiguration
      (force-reconfig test replica-set-db nodes removed)
    )
  )
)


(defn parse-options
  "Return a set of nodes based on the options
    :primary         - Choose only the primary node
    :minority        - Choose a random minority of nodes so that after remove the remaining members >= 3
    :majority        - Choose a random majority of nodes so that after remove the remaining members < 3
    ;; TODO:
    :all             - Choose all nodes
    :one             - Chooses a single random node
  "
  [test, options]
  (let
    [
      nodes (:nodes test)
      db (:db test)
      saft-num (- (count nodes) 3)
      minority-num (+ (rand-int saft-num) 1) ;; = 1 ~ saftnum
      majority-num (- (count nodes) (+ (rand-int 2) 1)) ;; = total number - 1/2
    ]
    (case options
      :primary    (set (db/primaries db test)) ;; here assume all nodes should be available
      :minority   (take minority-num (shuffle nodes))
      :majority   (take majority-num (shuffle nodes))
      ;; :one        (list (rand-nth nodes))
      ;; :all        nodes
    )
  )
)


(defn remove-members
  "Forcibly remove voting members from the replica set (i.e. kill mongod process) to simulate crashes
  As required by MongoDB, assume remove even numbers of members 
  "
  [test, options]
  (info "Start removing members")
  (let
    [
      nodes (:nodes test), ;; all nodes in a test
      replica-set-db (:db test)
      target (parse-options test options)
    ]
    (info "Removing " target " in remove-members")
    ;; apply kill on all the targets
    (jcontrol/on-nodes test target (partial db/kill! replica-set-db))
    ;; update status
    (dosync (ref-set crashing-status (set target)))
    (info "remove nodes " target " new crashing status is " (deref crashing-status))
  )
)


(defn member-nemesis
  "A nemesis for adding and removing member from the replica set with only voting members.
  
  Support 2 actions
  - add: add new voting members into the replica set
  - remove: remove existing voting members from the replica set
  
  As required by MongoDB:
  - A replica set should have at least 3 voting nodes.
  - A replica set should always have an odd number of voting members.
  - A replica set can have up to 50 members, but only up to 7 voting members, all other embers must be non-voting members.
  (details https://www.mongodb.com/docs/manual/core/replica-set-architectures/#determine-the-number-of-members)

  Adding and removing steps are stated on https://www.mongodb.com/docs/manual/tutorial/expand-replica-set/#requirements
  "
  [opts]

  (reify
    ;; Reflection defines "What :f functions does this nemesis support?"
    jnem/Reflection
    (fs [this] #{:add-members :remove-members})
  
    jnem/Nemesis
    ;; Setup the nemesis to work with the cluster. Returns the nemesis ready to be invoked.
    (setup! [this test]
      (info "Setting up member nemesis")
      (dosync (ref-set version-cnt 1)) ;; track the version number, version start from 1
      (dosync (ref-set member-id-cnt (- (count (:nodes test)) 1) )) ;; id starts from 0
      this
    )

    ;; Invoke - perform acture add and remove of members
    (invoke! [this test op]
      (assoc op :value
        (case (:f op)
          :add-members     (add-members test)
          :remove-members   (remove-members test (:value op))
        ))
    ) 

    ;; Teardown the nemesis when work is complete
    (teardown! [this test] (info "Tearing down member nemesis"))
  )
)


(defn member-generator
  "A generator for member nemesis."
  [opts]
  (let
    [
      remove-options (:targets (:member opts) [:primary :minority :majority]),
      rm (fn [_ _] {:type :info, :f :remove-members, :value (rand-nth remove-options)}),
      ad (fn [_ _] {:type :info, :f :add-members, :value :all})
    ]
    ;; A simple logic is to remove -> add -> remove -> add .... repeat
    (->>
      (jgen/flip-flop rm ad)
      (jgen/stagger (:interval opts 10))
    )
  )
)


(defn member-package
  "A combined nemesis package for adding and removing members from/to the replica set."
  ;; mongodb.clj line 67 sets most of input opts, specifically specified
  ;; :db, :nodes, :faults, :interval, and targets for each nemesis
  [opts]
  ;; Only return when required, i.e. --nemesis member
  (when ((:faults opts) :member)
    {
      :nemesis   (member-nemesis opts)
      :generator (member-generator opts)
      :perf      #{
        {:name "member", :start #{:remove-members}, :stop #{:add-members}, :color "#E9A0E6"}
      }
    })
)

