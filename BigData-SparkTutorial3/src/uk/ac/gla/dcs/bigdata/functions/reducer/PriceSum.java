package uk.ac.gla.dcs.bigdata.functions.reducer;

import org.apache.spark.api.java.function.ReduceFunction;

public class PriceSum implements ReduceFunction<Double>{

	private static final long serialVersionUID = 1551800280877618065L;

	@Override
	public Double call(Double v1, Double v2) throws Exception {
		return v1+v2;
	}

}
