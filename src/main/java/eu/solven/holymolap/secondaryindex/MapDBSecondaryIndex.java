package eu.solven.holymolap.secondaryindex;

public class MapDBSecondaryIndex {
//	public void index() {
//		DB db = DBMaker.heapDB().make();
//
//		// create secondary value (1:1 relation)
//		// secondary map gets auto updated
//		HTreeMap<Map<String, String>, Map<String, String>> persons = db.hashMapCreate("persons").makeOrGet();
//		HTreeMap<Map<String, String>, Map<String, String>> branches = db.hashMapCreate("branches").makeOrGet();
//
//		Bind.secondaryValue(persons, branches, new Fun.Function2<Map<String, String>, Map<String, String>, Map<String, String>>() {
//
//			@Override
//			public Map<String, String> run(Map<String, String> key, Map<String, String> value) {
//				return ImmutableMap.of("branchId", "someId");
//			}
//		});
//		// create secondary key (index) for age(N:1)
//		NavigableSet<Object[]> ages = new TreeSet<>();
//		Bind.secondaryKey(persons, ages, new Fun.Function2<Map<String, String>, Map<String, String>, Map<String, String>>() {
//
//			@Override
//			public Map<String, String> run(Map<String, String> key, Map<String, String> value) {
//				return ImmutableMap.of("branchId", "someId");
//			}
//		});
//		// // get all persons of age 32
//		for (Object[] id : Fun.filter(ages, 32)) {
//			// persons.get(o)
//		}
//		// Pe
//	}
}
