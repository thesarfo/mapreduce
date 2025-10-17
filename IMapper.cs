namespace mapreduce;

public interface IMapper{
    Dictionary<string, string> Map(string key, string value);
}

public class Mapper : IMapper
{
    public Dictionary<string, string> Map(string key, string value)
    {
        var output = new Dictionary<string, string>();

        var words = value.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        foreach (var word in words)
        {
            output[word.ToLower()] = "1";
        }

        return output;
    }
}