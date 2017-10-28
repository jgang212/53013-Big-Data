def printSquares(start: Int, end: Int): Unit = 
	for( a <- start to end)
	{
         println(a, "  ", a*a);
	}

printSquares(1, 5)