namespace mapreduce;

public interface IReducer{
    string Reduce(string key, IEnumerable<string> values);
}

public class Reducer : IReducer
{
    public string Reduce(string key, IEnumerable<string> values)
    {
        throw new NotImplementedException();
    }
}