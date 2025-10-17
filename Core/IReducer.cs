namespace mapreduce;

public interface IReducer{
    string Reduce(string key, IEnumerable<string> values);
}

public class Reducer : IReducer
{
    public string Reduce(string key, IEnumerable<string> values)
    {
        int count = values.Count();
        return count.ToString();
    }
}