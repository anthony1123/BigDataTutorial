package uk.ac.gla.dcs.bigdata.functions.reducer;

import org.apache.spark.api.java.function.ReduceFunction;

/**
 * This is the simplest example of a reducer, it just adds together
 * two integers, this is processed recursively for the whole dataset
 * to get a global sum.
 * @author Richard
 *
 */
public class IntSumReducer implements ReduceFunction<Integer>{

	private static final long serialVersionUID = 2056154699873405088L;

	@Override
	public Integer call(Integer v1, Integer v2) throws Exception {
		return v1+v2;
	}

}
