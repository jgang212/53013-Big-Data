def fibonacci(n : Int) : Int = 
{ 
    def fibonacci_tail(n: Int, a:Int, b:Int): Int = 
    {
        if (n == 0) { return a }
        else { return fibonacci_tail(n-1, b, a+b) } 
    }
    return fibonacci_tail(n, 0, 1)
}

for (a <- 1 to 10)
{
    println(fibonacci(a))
}