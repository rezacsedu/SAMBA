import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/*
 * @author Md. Rezaul Karim - 07/06/2016
 * 
 */

public class Combination {
	public static void main(String[] args) throws Exception {
		test();
	}
	
	public static void test() throws Exception {
		List<String> list = Arrays.asList("a", "b", "c", "d");
		System.out.println("list=" + list);
		List<List<String>> comb = findSortedCombinations(list);
		System.out.println(comb.size());
		System.out.println(comb);
	}	

	public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements) {
		List<List<T>> result = new ArrayList<List<T>>();
		for (int i = 0; i <= elements.size(); i++) {
			result.addAll(findSortedCombinations(elements, i));
		}
		return result;
	}

	public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements,
			int n) {
		List<List<T>> result = new ArrayList<List<T>>();
		if (n == 0) {
			result.add(new ArrayList<T>());
			return result;
		}
		List<List<T>> combinations = findSortedCombinations(elements, n - 1);
		for (List<T> combination : combinations) {
			for (T element : elements) {
				if (combination.contains(element)) {
					continue;
				}
				List<T> list = new ArrayList<T>();
				list.addAll(combination);
				if (list.contains(element)) {
					continue;
				}

				list.add(element);
				// sort items to avoid duplicate items; for example (a, b, c)
				// and
				// (a, c, b) might be counted as different items if not sorted
				Collections.sort(list);
				if (result.contains(list)) {
					continue;
				}
				result.add(list);
			}
		}
		return result;
	}
}