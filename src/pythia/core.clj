(ns pythia.core
  (:require [clojure.pprint :as pp]
            [pythia.rdf :as rdf]))

;; *** Data stores ***

(defprotocol Store
  "A Store stores triples and can scan itself for a pattern"
  (scan [db pattern] "Scan a database for a pattern")
  (add-triple [db triple] "Add a triple"))

(defn qvar
  "Query vars are used as variable in queries. In a pattern they can assume any value"
  [name]
  (with-meta {:name name} {:tag ::qvar}))

(defn qvar?
  "Tests if argument is a query var"
  [v]
  (= (:tag (meta v)) ::qvar))

(defn match
  "Returns true if the candidate matches the pattern. Query vars always match."
  [pattern candidate]
  (every? (fn [[p c]]
            (or (qvar? p)
                (= p c)))
          (map vector pattern candidate)))

(defn mask
  "Returns only the values that correspond to a query var."
  [pattern match]
  (filter identity
          (map (fn [p m]
                 (when (qvar? p) m))
               pattern
               match)))

;; Simple store that just stores all the triples in a big vector
(defrecord FlatStore [triples]
  Store
  (add-triple [_ triple]
    (FlatStore. (conj triples triple)))
  (scan [_ pattern]
    (->> triples
         (filter #(match pattern %))
         (map #(mask pattern %)))))


(defn walk-index
  "Used by TrieStore to look up values in a trie index."
  [index path]
  (if (empty? path)
    [index]                    ; if we've walked the path, we're done
    (let [a (first path)]
      (if (qvar? a)
        ;; In case of a qvar we need to expand into every key at this level
        (mapcat #(walk-index % (rest path)) (vals index))
        ;; Otherwise we descend into the trie
        (if-let [lvl (index a)]
          (recur lvl (rest path)))))))

;; Indexed store that stores the triples in two tries that correspond to different
;; orders
(defrecord TrieStore [eav ave]
  Store
  (add-triple [_ [e a v :as triple]]
    (let [eav (assoc-in eav [e a v] triple)
          ave (assoc-in ave [a v e] triple)]
      (TrieStore. eav ave)))
  (scan [_ [e a v :as pattern]]
    (map #(mask pattern %)
         (if (qvar? e)
           ;; Determine which of the two indices to use.
           (walk-index ave [a v e])
           (walk-index eav [e a v])))))


;; *** Basic binary join algorithm ***

(defn relation
  "Build a relation out of a pattern, which is simply a scan + a mapping from qvar to
   position in the pattern."
  [db pattern]
  {:index (pattern-index pattern)
   :scan (scan db pattern)})

(defn reindex
  "When joining 2 relations, reindex the qvar positions"
  [index1 index2 on]
  (let [c (count index1)]
    (into index1
          (map-indexed #(vector %2 (+ c %1))
                       (keep
                        (fn [[k _]]
                          (when (not (some #(= % k) on)) k))
                        (sort-by second index2))))))


(defn binary-join
  "Join 2 relations on a qvar"
  [rel1 rel2 on]
  (let [mask1 (map (:index rel1) on) ;create a mask of indices for each
        mask2 (map (:index rel2) on) ;qvar
        joinval (fn [mask fact] (map #(nth fact %) mask))
        remove-join-attrs (fn [mask fact]
                            (keep-indexed
                             (fn [i f]
                               (when (not (some #(= i %) mask)) f))
                             fact))
        jmap (reduce (fn [jmap fact]
                       (let [jval (joinval mask2 fact)
                             jset (jmap jval #{})]
                         (assoc jmap jval (conj jset fact))))
                     {}
                     (:scan rel2))
        joined (mapcat (fn [fact]
                         (let [join (jmap (joinval mask1 fact))]
                           (map #(concat fact (remove-join-attrs mask2 %))
                                join)))
                       (:scan rel1))
        index (reindex (:index rel1) (:index rel2) on)]
    {:index index
     :scan joined}))

(defn cross
  "Cartesian product on 2 relations"
  [rel1 rel2]
  (let [scan1 (:scan rel1)
        scan2 (:scan rel2)]
    {:index (reindex (:index rel1) (:index rel2) [])
     :scan (for [f1 scan1
                 f2 scan2]
             (concat f1 f2))}))

(defn select
  "Select the qvars we're interested in from a joined result"
  [rel qvars]
  (let [indices (filter identity  (map (:index rel) qvars))]
    (map (fn [row]
           (map #(nth row %) indices))
         (:scan rel))))

(defn common-qvars
  "Get the common qvars from two relations to join on"
  [{index1 :index} {index2 :index}]
  (filter index1 (keys index2)))

(defn binary-join-query
  "Query a db"
  [db selection clauses]
  (let [rel (reduce (fn [rel clause]
                      (if-let [join-vars (common-qvars rel clause)]
                        (binary-join rel clause join-vars)
                        (cross rel clause)))
                    (map (partial relation db) clauses))]
    (select rel selection)))

;; *** Query macro ***

(defn parse-query
  "Helper function to parse the components of a query into a map"
  [q]
  (->> q
       (partition-by keyword?)
       (partition 2)
       (map (fn [[[k] v]] [k v]))
       (into {})))

(defn replace-qvars
  "Replace ?vars with query vars"
  [sexp]
  (cond
    (sequential? sexp) (map replace-qvars sexp)
    (and (symbol? sexp)
         (= (first (str sexp)) \?)) (qvar (str sexp))
    :default sexp))

(defmacro query [& q]
  (let [{selection :find [db] :in clauses :where :as q} (parse-query q)
        selection (vec (replace-qvars selection))
        clauses (mapv vec (replace-qvars clauses))
        [algo] (q :algo [binary-join-query])]
    `(~algo ~db ~selection ~clauses)))


;; *** Worst case join algorithm ***


(defn intersect
  "Intersect a number of sets using hashes. By using the smallest set, we can
   guarantee a runtime proportional to the smallest set, which means we're efficient
   on skewed data."
  [sets]
  (let [sets (sort-by count sets) ;; sort by number of items
        driver (first sets) ;; compare against the smallest set
        sets (rest sets)]
    (filter (fn [v]
              (every? #(% v) sets))
            (keys driver))))

(defn trie
  "Turn a list of facts into a trie"
  [facts order]
  (reduce
   (fn [trie fact]
     (let [ordered-fact (mapv #(nth fact %) order)]
       (assoc-in trie ordered-fact {})))
   {}
   facts))

(defn qvar-order
  "Get a global ordering of qvars that all relations will follow"
  [patterns]
  (->> patterns
       (mapcat #(filter qvar? %))
       (distinct)
       (map-indexed #(vector %2 %1))
       (into {})))

(defn pattern-qvars
  "Get the qvars in the order they occur in the pattern"
  [pattern]
  (filter qvar? pattern))

(defn init-rels
  "Initiate the relations based on patterns. This follows the global order."
  [db patterns global-order]
  (map
   (fn [pattern]
     (let [qvars (->> (filter qvar? pattern)
                      (map-indexed #(vector %2 %1))
                      (sort-by #(global-order (first %))))
           order (map second qvars)]
       {:trie (trie (scan db pattern) order)
        :index (map first qvars)}))
   patterns))

(defn constrain
  "Constrain a relation by a certain prefix"
  [rel prefix]
  (loop [index (:index rel)
         trie (:trie rel)
         pre prefix]
    (if (empty? pre)
      trie
      (let [[k v] (first pre)
            qvar (first index)]
        (if (= k qvar)
          (recur (rest index) (trie v) (rest pre))
          (recur index trie (rest pre)))))))


(defn hypergraph
  "Build a hypergraph of the relations. This is simply a map from query var to the
   relations that contain that query var."
  [rels]
  (reduce
   (fn [hgraph rel]
     (reduce (fn [hgraph attr]
               (assoc hgraph attr
                      (conj (hgraph attr []) rel)))
             hgraph
             (:index rel)))
   {}
   rels))

(defn worst-join
  "Worst case join establishes a global order for the query vars. For every query var
   it then joins all the relations that contain that query var."
  [db patterns]
  (let [global-order (qvar-order patterns) ;; global order of qvars
        rels (init-rels db patterns global-order) ;; build tries out of patterns
        hgraph (hypergraph rels) ;; build a hypergraph out of the tries
        join-on-attr (fn join-on-attr [attrs prefix] ;; join relations on a qvar
                       (if (empty? attrs)
                         ;; if no more qvars, we're done
                         [(map second prefix)]
                         ;; Otherwise take the first qvar and constrain all the
                         ;; relations to the prefix. The prefix locks every attribute
                         ;; to a value. Effectively this descends into the tries.
                         ;; Then intersect the constrained tries and determine the
                         ;; next level of prefixes by locking the current qvar to the
                         ;; result of the join.
                         (let [attr (first attrs)
                               tries (map #(constrain % prefix) (hgraph attr))
                               intersection (intersect tries)
                               prefixes (map #(conj prefix [attr %]) intersection)]
                           (mapcat #(join-on-attr (rest attrs) %) prefixes))))]
    ;; Start the join with an empty prefix
    (join-on-attr (map first (sort-by second global-order)) [])))

(defn worst-case
  "Worst case join algorithm"
  [db selection clauses]
  (let [global-order (qvar-order clauses)]
    (map (fn [row]
           (map #(nth row (global-order %)) selection))
         (worst-join db clauses))))

;; Test queries based on the LUBM benchmark dataset

(def uni-triples (concat
                  [["AssistantProfessor" "subclassOf" "Professor"]
                   ["AssociateProfessor" "subclassOf" "Professor"]
                   ["FullProfessor" "subclassOf" "Professor"]]
                  (rdf/read-rdf-file "University0_0.owl")))
(def uni-flat (FlatStore. uni-triples))
(def uni-trie (reduce add-triple (TrieStore. {} {}) uni-triples))

(defn query1
  ([db]
   (query1 db worst-case))
  ([db algo]
   (query :find ?x :in db :algo worst-case
          :where
          [?x "type" "GraduateStudent"]
          [?x "takesCourse" "GraduateCourse0"])))

(defn query2
  ([db]
   (query2 db worst-case))
  ([db algo]
   (query :find ?x ?y ?z :in db :algo algo
          :where
          [?x "type" "GraduateStudent"]
          [?y "type" "University"]
          [?z "type" "Department"]
          [?x "memberOf" ?z]
          [?z "subOrganizationOf" ?y]
          [?x "undergraduateDegreeFrom" ?y])))

(defn query3
  ([db]
   (query3 db worst-case))
  ([db algo]
   (query :find ?x :in db :algo algo
          :where
          [?x "type" "Publication"]
          [?x "publicationAuthor" "AssistantProfessor0"])))

(defn query4
  ([db]
   (query4 db worst-case))
  ([db algo]
   (query :find ?y2 :in db :algo algo
          :where
          [?prof "subclassOf" "Professor"]
          [?x "type" ?prof]
          [?x "worksFor" "www.Department0.University0.edu"]
          [?x "name" ?y1]
          [?x "emailAddress" ?y2]
          [?x "telephone" ?y3])))
