(ns jepsen.mongodb.member-nem
    "A nemesis about adding and removing members from replica set"
    (:require   [clojure.tools.logging :refer :all]
                [clojure.set :as cset]
                [jepsen [nemesis :as jnem]
                        [net :as jnet]
                        [util :as jutil]]
                [jepsen.generator :as jgen]
                [jepsen.mongodb.db :as db]))


;; use a set to record the removed nodes
(def crashing-status (
  ref #{}
  ;; :validator pos? ;; there might need a validator
)) 


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
          rnd (rand-int (+ cnt 1))  ;; rnd = 0 ~ cnt, cnt itself should be even
          num (even? rnd rnd (+ rnd 1)) ;; if rnd is odd, add rnd + 1
          ;; add number must be 0 ~ cnt and even
          target (take num (shuffle removed))
        ]
        ;; update status
        (dosync (ref-set crashing-status (cset/difference removed (set target))))
        (info "Add member " target " new crashing status is " (deref crashing-status))
        ;; TODO: add members as steps
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
          rnd (rand-int (+ avail-cnt 1)) ;; rnd = 0 ~ avail-cnt
          num (even? rnd rnd (- rnd 1)) ;; when rnd is odd, remove rnd - 1
          ;; remove number must be 1 ~ avail-cnt and even
          target (take num (shuffle avail)) ;; randomly choose from nodes
        ]
        ;; update status
        (dosync (ref-set crashing-status (cset/union removed (set target))))
        (info "remove nodes " target " new crashing status is " (deref crashing-status))
        ;; TODO: should just kill these nodes
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
    (setup! [this test] (info "Setting up member nemesis") this)

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
      db (:db opts),
      nodes (:nodes opts),
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

