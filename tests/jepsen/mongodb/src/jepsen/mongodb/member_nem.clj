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

(defn construct-config
  "Construct a new config based on a old config

  "
  [test, members, old-config]

  {
    :_id (:replica-set-name test "rs_jepsen")
    ;; :configsvr (mdb/config-server? test)
    ;;  ; See https://docs.mongodb.com/manual/reference/replica-configuration/#rsconf.settings.catchUpTimeoutMillis
    ;; :settings {
    ;;   :heartbeatTimeoutSecs       1,
    ;;   :electionTimeoutMillis      1000,
    ;;   :catchUpTimeoutMillis       1000,
    ;;   :catchUpTakeoverDelayMillis 3000
    ;; }
    ;; Each replica set member must have a unique _id. Avoid re-using _id values even if no members[n] entry is using that _id in the current configuration.
    ;; Once set, you cannot change the _id of a member.    
    ;; :members (->>
    ;;   members
    ;;   (map-indexed (fn [i node]
    ;;                               {:_id  i
    ;;                                :priority (if (hidden node)
    ;;                                            0
    ;;                                            (- (count (:nodes test)) i))
    ;;                                :votes    (if (hidden node)
    ;;                                            0
    ;;                                            1)
    ;;                                :hidden   (boolean (hidden node))
    ;;                                :host     (str node ":"
    ;;                                               (if (config-server? test)
    ;;                                                 client/config-port
    ;;                                                 client/shard-port))})))
    }
)


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
                ;; new-version (+ (:version old-config) 1)
                ;; TODO: get the member list by removing 1 member
                new-members (vec (filter #(not= (mcl/addr->node (:host %)) n) (:members old-config)))
                ;; TODO: construct new config
                new-config {:_id id, :version new-version, :members new-members}
              ]
              ;; TODO: call reconfig
              (try
                ;; the below command should act as rs.remove()
                ;; This function will disconnect the shell briefly and forces a reconnection
                ;; the shell will display an error even if this command succeeds
                (mcl/admin-command! conn { :replSetReconfig new-config })
                (catch Exception e (info "Reconfig should have completed, the error is\n" e) nil)
              )
              ;; TODO: update version cnt
              (dosync (ref-set version-cnt new-version))
              (info "cleaned up for " n)
            )
          )
        )
      )
    )
  )
)


(defn add-with-reconfig
  ""
  [test, replica-set-db, nodes, target]
  (let
    [
      primary (->>
        (cset/difference (set nodes) target) ;; calculate remaining alive nodes
        (assoc test :nodes) ;; for following code, we only need to check alive nodes
        (db/primaries replica-set-db)
        (first) ;; get only the first result
      )
      port (if (mdb/config-server? test) mcl/config-port mcl/shard-port)

    ]

    ;; should 


    ;; db.adminCommand(
    ;;   {
    ;;     replSetGetConfig: 1,
    ;;     commitmentStatus: <boolean>,
    ;;     comment: <any>
    ;;   }
    ;; )
    ;; (info "New primary after rs.remove is " primary)
    ;; ;; (.close (mcl/await-open primary port)) ;; wait for primary to connect
    (with-open [ conn (mcl/open primary port) ]
      (info "In add-with-reconfig, Current config:\n\n "  (mcl/admin-command! conn { :replSetGetConfig 1 }) "\n\n") ;; get current config
    )
    
  )
)


(defn add-members
  "Add voting members into the replica set.
  As required by MongoDB, assume add even numbers of members.
  "
  [test]
  (info "joining the replica set" )
  (let
    [
      removed (deref crashing-status) ;; removed count should always be even
      cnt (count removed)
    ]

    (if (pos? cnt)
      ;; if there are removed members, add some back
      (let
        [
          rnd (+ (rand-int cnt) 1),  ;; rnd = 1 ~ cnt, cnt itself should be even
          num (if (even? rnd) rnd (+ rnd 1)), ;; ensure add > 0 and even
          target (take num (shuffle removed)),
          replica-set-db (:db test),
          nodes (:nodes test)
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
        ;; (configure! test node) ;; seems can skip config since it should have been setup properly
        ;; (start! test node) ;;
        ;; (join! test node)
        ;; (jcontrol/on-nodes test target (partial db/kill! replica-set-db))

      )
      (info "No adding any member")
    )
  )
)


(defn remove-members
  "Forcibly remove voting members from the replica set (i.e. kill mongod process) to simulate crashes
  As required by MongoDB, assume remove even numbers of members 
  "
  [test]
  (info "Start removing members")
  (let
    [
      nodes (:nodes test), ;; all nodes in a test
      removed (deref crashing-status),
      avail (cset/difference (set nodes) removed), ;; still available nodes
      avail-cnt (- (count avail) 3) ;; # of nodes that can be removed
    ]
    (info "have " avail-cnt "members can be removed. Current crashing status is ", removed)
    (if (pos? avail-cnt)
      ;; if there are members available to remove, then do a random remove of even # of members
      (let
        [
          rnd (+ (rand-int avail-cnt) 1), ;; rnd = 1 ~ avail-cnt, where avail-cnt should be even
          num (if (even? rnd) rnd (+ rnd 1)), ;; ensure remove > 0 and even
          target (take num (shuffle avail)) ;; randomly choose from nodes
          replica-set-db (:db test)
        ]
        ;; update status
        (dosync (ref-set crashing-status (cset/union removed (set target))))
        (info "remove nodes " target " new crashing status is " (deref crashing-status))
        ;; apply kill on all the targets
        (jcontrol/on-nodes test target (partial db/kill! replica-set-db))
      )
      ;; otherwise, just not removing any node
      (info "Not removing any node")
    )
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
      (dosync (ref-set version-cnt 1))) ;; track the version number
      this
    )

    ;; Invoke - perform acture add and remove of members
    (invoke! [this test op]
      (assoc op :value
        (case (:f op)
          :add-members     (add-members test) ;; TODO
          :remove-members   (remove-members test) ;; TODO
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
      ;; db (:db opts),
      ;; nodes (:nodes opts),
      rm (fn [_ _] {:type :info, :f :remove-members}),
      ad (fn [_ _] {:type :info, :f :add-members})
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

