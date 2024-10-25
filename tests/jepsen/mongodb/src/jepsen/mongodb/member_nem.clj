(ns jepsen.mongodb.member-nem
    "A nemesis about adding and removing members from replica set"
    (:require   [clojure.tools.logging :refer :all]
                [jepsen [nemesis :as jnem]
                        [net :as jnet]
                        [util :as jutil]]
                [jepsen.generator :as jgen]
                [jepsen.mongodb.db :as db]))

(defn add-members
  "Add voting members into the replica set.
  As required by MongoDB, assume add even numbers of members.
  "
  [targets]
  (info targets "joining the replica set" )
)

(defn force-remove-members
  "Forcibly remove voting members from the replica set (i.e. kill mongod process) to simulate crashes
  As required by MongoDB, assume remove even numbers of members 
  "
  [targets]
  (info targets "leaving the replica set" )
)

;; use a set to record the removed nodes
(def crashing-status (
  ref #{}
  ;; :validator pos? ;; there might need a validator
)) 

;; Only here can find all nodes at "test"
;; then here is the place where we should tracking the removed nodes
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
    n/Reflection
    (fs [_] [:add-member :remove-member]))

    n/Nemesis
    ;; Setup the nemesis to work with the cluster. Returns the nemesis ready to be invoked.
    (setup! [this test] (info node "Setting up member nemesis"))

    ;; Invoke - perform acture add and remove of members
    (invoke! [this test op]
      (assoc op :value
        (case (:f op)
          :add-member     (add-members test) ;; TODO
          :remove-member   (force-remove-members test) ;; TODO
        ))) 

    ;; Teardown the nemesis when work is complete
    (teardown! [this test] (info node "Setting up member nemesis"))
)

;; Only here can find all nodes at "test"
;; then here is the place where we should tracking the removed nodes
(defn member-generator
  "A generator for member nemesis."
  [opts]
  (let [
    db (:db opts),
    
    rm {:type :info, :f :force-remove-members, :value (rand-nth targets)}, ;; TODO - need to choose from available nodes
    st {:type :info, :f :add-members, :value (rand-nth targets)}, ;; TODO - need to choose from removed nodes
  ])
  ;; A simple logic is to remove -> add -> remove -> add .... repeat
  (->>
    (gen/flip-flop rm st)
    (gen/stagger (:interval opts default-interval))
  )
)


(defn member-package
  "A combined nemesis package for adding and removing members from/to the replica set."
  ;; mongodb.clj line 67 sets most of input opts
  [opts]
  ;; Only return when required, i.e. --nemesis member
  (when ((:faults opts) :member)
    {
      :nemesis   (member-nemesis opts)
      :generator (member-generator opts)
      :perf      #{
        {:name "add-members", :fs [:add-members], :color "#E9A0E6"}
        {:name "remove-members", :fs [:force-remove-members], :color "#ACA0E9"}
      }})
)

