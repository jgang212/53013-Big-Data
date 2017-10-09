namespace java edu.uchicago.mpcs53013.weatherAnalytics

struct Data {
	1: required WeatherStation station;
	2: required Time time;
	3: required DataUnit data;
}

struct WeatherStation {
	1: required i64 STN;
	2: required i64 WBAN;
}

struct Time {
	1: required i64 YEAR;
	2: required i64 MODA;
}

union DataUnit {
	1: Temp temp;
	2: DewPoint dew;
	3: SLPressure slp;
	4: StPressure stp;
	5: Visibility vis;
	6: WindSpeed wdsp;
	7: Precipitation prec;
	8: double SNDP;
	9: i64 FRSHTT;
}

struct Temp {
	1: required MeanTemp meantemp;
	2: required MaxTemp maxtemp;
	3: required MinTemp mintemp;
}

struct MeanTemp {
	1: required double TEMP;
	2: required i64 Count;
}

struct MaxTemp {
	1: required double MAX;
	2: required string Flag;
}

struct MinTemp {
	1: required double MIN;
	2: required string Flag;
}

struct DewPoint {
	1: required double DEWP;
	2: required i64 Count;
}

struct SLPressure {
	1: required double SLP;
	2: required i64 Count;
}

struct StPressure {
	1: required double STP;
	2: required i64 Count;
}

struct Visibility {
	1: required double VISIB;
	2: required i64 Count;
}

struct WindSpeed {
	1: required double WDSP;
	2: required i64 Count;
	3: required double MXSPD;
	4: required double GUST;
}

struct Precipitation {
	1: required double PRCP;
	2: required string Flag;
}