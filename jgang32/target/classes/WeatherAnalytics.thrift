namespace java edu.uchicago.mpcs53013

struct WeatherData {
	1: required i64 STN;
	2: required i64 WBAN;
	3: required i64 YEARMODA;
	4: required double TMP;
	5: required i64 TMPCount;
	6: required double DEWP;
	7: required i64 DEWPCount;
	8: required double SLP;
	9: required i64 SLPCount;
	10: required double STP;
	11: required i64 STPCount;
	12: required double VISIB;
	13: required i64 VISIBCount;
	14: required double WDSP;
	15: required i64 WDSPCount;
	16: required double MAXSPD;
	17: required double GUST;
	18: required double MAX;
	19: required string MAXFlag;
	20: required double MIN;
	21: required string MINFlag;
	22: required double PRCP;
	23: required string PRCPFlag;
	24: required double SNDP;
	25: required string FRSHTT;
}