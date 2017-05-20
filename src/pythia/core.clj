(ns pythia.core
  (:require [clojure.pprint :as pp]))

(defn init-db []
  {:counter 0 :facts []})

(defn add-facts [{:keys [counter facts] :as db} record]
  (let [id (inc counter)]
    (-> db
        (assoc :counter id)
        (assoc :facts
               (concat facts
                       (map (fn [[k v]]
                              [id k v])
                            record))))))


(defn qvar [name]
  (with-meta {:name name} {:tag ::qvar}))

(defn qvar? [v]
  (= (:tag (meta v)) ::qvar))

(defn match [pattern candidate]
  (every? (fn [[p c]]
            (or (qvar? p)
                (= p c)))
          (map vector pattern candidate)))

(defn mask [pattern match]
  (filter identity
          (map (fn [p m]
                 (when (qvar? p) m))
               pattern
               match)))

(defn scan
  "Scan a database for matches against a pattern"
  [db pattern]
  (->> (:facts db)
       (filter (partial match pattern))
       (map (partial mask pattern))))

(defn pattern-index [pattern]
  (into {}
        (map-indexed #(vector %2 %1) (filter qvar? pattern))))

(defn relation [db pattern]
  {:index (pattern-index pattern)
   :scan (scan db pattern)})


(defn reindex [index1 index2 on]
  (let [c (count index1)]
    (into index1
          (map-indexed #(vector %2 (+ c %1))
                       (keep
                        (fn [[k _]]
                          (when (not (some #(= % k) on)) k))
                        (sort-by second index2))))))


(defn join
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

(defn select [rel qvars]
  (let [indices (filter identity  (map (:index rel) qvars))]
    (map (fn [row]
           (map #(nth row %) indices))
         (:scan rel))))

(defn common-qvars
  "Get the common qvars from two relations to join on"
  [{index1 :index} {index2 :index}]
  (filter index1 (keys index2)))

(defn query*
  "Query a db"
  [db selection clauses]
  (let [rel (reduce (fn [rel clause]
                      (if-let [join-vars (common-qvars rel clause)]
                        (join rel clause join-vars)
                        (cross rel clause)))
                    (map (partial relation db) clauses))]
    (select rel selection)))

(defn parse-query [q]
  (when (= (first q) :find)
    (let [[selection remainder] (split-with #(not= :in %) (rest q))
          [db remainder] (split-with #(not= :where %) (rest remainder))
          clauses (rest remainder)]
      [selection (first db) clauses])))

(defn replace-qvars [sexp]
  (cond
    (sequential? sexp) (map replace-qvars sexp)
    (and (symbol? sexp)
         (= (first (str sexp)) \?)) (qvar (str sexp))
    :default sexp))

(defmacro query [& q]
  (let [[selection db clauses] (parse-query q)
        selection (vec (replace-qvars selection))
        clauses (mapv vec (replace-qvars clauses))]
    `(query* ~db ~selection ~clauses)))


(def db (-> (init-db)
            (add-facts {:person/name "Adam"})
            (add-facts {:pet/name "Jasper" :pet/owner 1 :pet/type :cat})
            (add-facts {:pet/name "Marley" :pet/owner 1 :pet/type :cat})
            (add-facts {:person/name "Mike"})
            (add-facts {:pet/name "Marley" :pet/owner 4 :pet/type :dog})
            (add-facts {:person/name "Kees" :person/friend 1})
            (add-facts {:person/name "Steve" :person/friend 1})))


;; (def rel1 (relation db [(qvar :p) :pet/name "Marley"]))
;; (def rel2 (relation db [(qvar :p) :pet/owner (qvar :o)]))
;; (def rel3 (relation db [(qvar :o) :person/name (qvar :n)]))

;; We'd like to be able to write the following:
;; (query :find ?name
;;        :where [[?p :pet/name "Marley"]
;;                [?p :pet/owner ?o]
;;                [?o :person/name ?name]])


(query* db [(qvar :n)]
        [[(qvar :p) :pet/name "Marley"]
         [(qvar :p) :pet/owner (qvar :o)]
         [(qvar :o) :person/name (qvar :n)]])

;; We'd like to be able to do the following
;; (query :find ?n1 ?n2
;;        :where
;;        [?e1 :person/name ?n1]
;;        [?e2 :person/name ?n2]
;;        [?e1 :person/friend ?e2])

(def friends
  {:counter 0
   :facts [[1 :name "Adam"]
           [2 :name "Bram"]
           [3 :name "Charlie"]
           [4 :name "Daan"]
           [5 :name "Eva"]
           [6 :name "Froukje"]
           [7 :name "Geert"]
           [1 :friend 3]
           [2 :friend 3]
           [3 :friend 1]
           [3 :friend 2]
           [4 :friend 7]
           [5 :friend 6]]})

(def frel1 (relation friends [(qvar :e1) :name (qvar :n1)]))
(def frel2 (relation friends [(qvar :e2) :name (qvar :n2)]))
(def frel3 (relation friends [(qvar :e1) :friend (qvar :e2)]))


;; select from A, B, C where A.y = B.y and B.z = C.z and C.x = A.x
;; (query :find ?a ?b ?c
;;        :where
;;        [?a :x ?x]
;;        [?a :y ?y]
;;        [?b :y ?y]
;;        [?b :z ?z]
;;        [?c :z ?z]
;;        [?c :x ?x])



