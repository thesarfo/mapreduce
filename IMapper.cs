namespace mapreduce;

public interface IMapper{
    List<KeyValuePair<string, string>> Map(string key, string value); 
}

public class Mapper : IMapper
{
    public List<KeyValuePair<string, string>> Map(string key, string value)
    {
        var output = new List<KeyValuePair<string, string>>();

        var words = value.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        foreach (var word in words)
        {
            output.Add(new KeyValuePair<string, string>(word.ToLower(), "1"));
        }

        return output;
    }
}