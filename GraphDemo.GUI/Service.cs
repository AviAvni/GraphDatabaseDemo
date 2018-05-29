using Google.Maps;
using Neo4j.Driver.V1;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphDemo.GUI
{
    public class Service
    {
        static IDriver driver = GraphDatabase.Driver("bolt://localhost", AuthTokens.Basic("neo4j", "123456"));

        public static Stop[] GetStops(string desc)
        {
            var condition = string.Join("AND ", desc.Split(' ').Select(w => $"s.desc CONTAINS '{w}'"));
            using (var session = driver.Session())
            {
                return session.Run($@"
MATCH (s:Stop) 
WHERE {condition}
RETURN s").Select(r =>
                {
                    var node = r["s"].As<INode>();
                    return new Stop()
                    {
                        Id = node.Properties["id"].ToString(),
                        Name = node.Properties["name"].ToString(),
                        Desc = node.Properties["desc"].ToString()
                    };
                }).ToArray();
            }
        }

        public static Response CreatePlan(string source, string target, string time)
        {
            if (source == null || target == null || time == null)
                return null;

            using (var session = driver.Session())
            {
                // 🚏 -> 🚍 -> 🚏
                var directQuery = $@"
MATCH (s1:Stop)<-[:LOCATED_AT]-(st1:Stoptime)-[:PRECEDES*]->(st2:Stoptime)-[:LOCATED_AT]->(s2:Stop),
      (st1)-[:PART_OF_TRIP]->(t:Trip)-[:USES]->(r:Route)<-[:OPERATES]-(a:Agency)
WHERE s1.id = {{source}} AND 
      s2.id = {{target}} AND 
      st1.arrival_time.hour = toInteger({{time}}) 
RETURN [a, r, t, s1, st1, s2, st2] as nodes
LIMIT 1";

                // 🚏 -> 🚍 -> 🚏 -> 🚍 -> 🚏
                var notDirect1Query = $@"
MATCH (s1:Stop)<-[:LOCATED_AT]-(st1:Stoptime)-[:PRECEDES*]->(st2:Stoptime)-[:LOCATED_AT]->(s2:Stop)<-[:LOCATED_AT]-(st3:Stoptime)-[:PRECEDES*]->(st4:Stoptime)-[:LOCATED_AT]->(s3:Stop), 
      (st1)-[:PART_OF_TRIP]->(t:Trip)-[:USES]->(r:Route)<-[:OPERATES]-(a:Agency)
WHERE s1.id = {{source}} AND 
      s3.id = {{target}} AND 
      st1.arrival_time.hour = toInteger({{time}}) AND 
      duration.inSeconds(st2.arrival_time, st3.arrival_time).seconds < 30* 60 AND 
      st2.arrival_time < st3.arrival_time
RETURN [a, r, t, s1, st1, s2, st2, s2, st3, s3, st4] as nodes
LIMIT 1";

                // 🚏 -> 🚍 -> 🚏 -> 🚶 -> 🚏 -> 🚍 -> 🚏
                var notDirect2Query = $@"
MATCH (s1:Stop)<-[:LOCATED_AT]-(st1:Stoptime)-[:PRECEDES*]->(st2:Stoptime)-[:LOCATED_AT]->(s2:Stop), 
      (s3:Stop)<-[:LOCATED_AT]-(st3:Stoptime)-[:PRECEDES*]->(st4:Stoptime)-[:LOCATED_AT]->(s4:Stop), 
      (st1)-[:PART_OF_TRIP]->(t1:Trip)-[:USES]->(r1:Route)<-[:OPERATES]-(a1:Agency),
      (st3)-[:PART_OF_TRIP]->(t2:Trip)-[:USES]->(r2:Route)<-[:OPERATES]-(a2:Agency)
WHERE s1.id = {{source}} AND 
      s4.id = {{target}} AND 
      distance(s2.location, s3.location) < 500 AND
      st1.arrival_time.hour = toInteger({{time}}) AND 
      duration.inSeconds(st2.arrival_time, st3.arrival_time).seconds < 30*60 AND 
      st2.arrival_time < st3.arrival_time
RETURN [a1, r1, t1, s1, st1, s2, st2, a2, r2, t2, s3, st3, s4, st4] as nodes
LIMIT 1";

                var parameters = new Dictionary<string, object>()
                {
                    [nameof(source)] = source,
                    [nameof(target)] = target,
                    [nameof(time)] = time
                };

                return
                    RunQuery(session, directQuery, parameters) ??
                    RunQuery(session, notDirect1Query, parameters) ??
                    RunQuery(session, notDirect2Query, parameters);
            }
        }

        private static Response RunQuery(ISession session, string query, Dictionary<string, object> parameters)
        {
            var result = session.Run(query, parameters).ToArray();
            var plan = result
                .Select(r =>
                    r["nodes"]
                        .As<List<INode>>()
                        .Select(n => FormatNode(n))
                        .ToArray())
                .FirstOrDefault();

            var markers = result
                .Select(r =>
                    r["nodes"]
                        .As<List<INode>>()
                        .Where(n => n.Labels.Contains("Stop"))
                        .Select(n => n["location"].As<Point>())
                        .Select(p => new LatLng(p.Y, p.X))
                        .ToArray())
                .FirstOrDefault();

            return new Response(plan, markers);
        }

        private static string FormatNode(INode n)
        {
            switch (n.Labels[0])
            {
                case "Stop":
                    return "תחנה " + n.Properties["name"].ToString();
                case "Stoptime":
                    return "מגיע ב " + n.Properties["arrival_time"].ToString();
                case "Trip":
                    return "נסיעה " + n.Properties["headsign"].ToString();
                case "Route":
                    return "קו " + n.Properties["short_name"].ToString() + " " + n.Properties["long_name"].ToString();
                case "Agency":
                    return "מפעיל " + n.Properties["name"].ToString();
                default:
                    return "Error";
            }
        }

    }
}
