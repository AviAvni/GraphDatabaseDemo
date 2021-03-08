using Google.Maps;
using Neo4j.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace GraphDemo.GUI
{
    public class Service
    {
        static IDriver driver = GraphDatabase.Driver("bolt://localhost", AuthTokens.Basic("neo4j", "123456"));

        // 🚏 -> 🚍 -> 🚏
        private const string DirectQuery = @"
MATCH (s1:Stop)<-[:LOCATED_AT]-(st1:Stoptime)-[:PRECEDES*]->(st2:Stoptime)-[:LOCATED_AT]->(s2:Stop),
      (st1)-[:PART_OF_TRIP]->(t:Trip)-[:USES]->(r:Route)<-[:OPERATES]-(a:Agency)
WHERE s1.id = $source AND 
      s2.id = $target AND 
      st1.arrival_time.hour = toInteger($time) 
RETURN [a, r, t, s1, st1, s2, st2] as nodes
LIMIT 1";

        // 🚏 -> 🚍 -> 🚏 -> 🚍 -> 🚏
        private const string OneSwitchNoWalkingQuery = @"
MATCH (s1:Stop)<-[:LOCATED_AT]-(st1:Stoptime)-[:PRECEDES*]->(st2:Stoptime)-[:LOCATED_AT]->(s2:Stop)<-[:LOCATED_AT]-(st3:Stoptime)-[:PRECEDES*]->(st4:Stoptime)-[:LOCATED_AT]->(s3:Stop), 
      (st1)-[:PART_OF_TRIP]->(t:Trip)-[:USES]->(r:Route)<-[:OPERATES]-(a:Agency),
      (st3)-[:PART_OF_TRIP]->(t2:Trip)-[:USES]->(r2:Route)<-[:OPERATES]-(a2:Agency)
WHERE s1.id = $source AND 
      s3.id = $target AND 
      st1.arrival_time.hour = toInteger($time) AND 
      duration.inSeconds(st2.arrival_time, st3.arrival_time).seconds < 30*60 AND 
      st2.arrival_time < st3.arrival_time
RETURN [a, r, t, s1, st1, s2, st2, a2, r2, t2, s2, st3, s3, st4] as nodes
LIMIT 1";

        // 🚏 -> 🚍 -> 🚏 -> 🚶 -> 🚏 -> 🚍 -> 🚏
        private const string OneSwitchLessThen100MetersQuery = @"
MATCH (s1:Stop)<-[:LOCATED_AT]-(st1:Stoptime)-[:PRECEDES*]->(st2:Stoptime)-[:LOCATED_AT]->(s2:Stop), 
      (s3:Stop)<-[:LOCATED_AT]-(st3:Stoptime)-[:PRECEDES*]->(st4:Stoptime)-[:LOCATED_AT]->(s4:Stop), 
      (st1)-[:PART_OF_TRIP]->(t1:Trip)-[:USES]->(r1:Route)<-[:OPERATES]-(a1:Agency),
      (st3)-[:PART_OF_TRIP]->(t2:Trip)-[:USES]->(r2:Route)<-[:OPERATES]-(a2:Agency)
WHERE s1.id = $source AND 
      s4.id = $target AND 
      distance(s2.location, s3.location) < 100 AND
      st1.arrival_time.hour = toInteger($time) AND 
      duration.inSeconds(st2.arrival_time, st3.arrival_time).seconds < 30*60 AND 
      st2.arrival_time < st3.arrival_time
RETURN [a1, r1, t1, s1, st1, s2, st2, a2, r2, t2, s3, st3, s4, st4] as nodes
LIMIT 1";

        public static Stop[] GetStops(string city, string street, bool isTrain)
        {
            var conditions = new List<string>();
            if (!string.IsNullOrEmpty(city))
            {
                conditions.Add($"s.desc CONTAINS 'עיר: {city}'");
            }
            if (isTrain)
            {
                conditions.Add($"s.desc CONTAINS 'רכבת'");
            }
            if (!string.IsNullOrEmpty(street))
            {
                conditions.Add($"s.desc CONTAINS 'רחוב:{street}'");
            }

            var session = driver.Session();
            var query = $@"
MATCH (s:Stop) 
WHERE {string.Join(" AND ", conditions)}
RETURN s
ORDER BY s.name";

            var records = session.Run(query).ToArray();
            return records.Select(r =>
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

        public static async Task<Response> CreatePlanAsync(string source, string target, string time, PlanType planType)
        {
            if (source == null || target == null || time == null)
                return null;

            var session = driver.AsyncSession();
            var parameters = new Dictionary<string, object>()
            {
                [nameof(source)] = source,
                [nameof(target)] = target,
                [nameof(time)] = time
            };

            return await RunQueryAsync(session, planType, parameters);
        }

        private static async Task<Response> RunQueryAsync(IAsyncSession session, PlanType planType, Dictionary<string, object> parameters)
        {
            var query = GetQuery(planType);
            var result = await (await session.RunAsync(query, parameters)).ToListAsync();
            var plan = result
                .Select(r =>
                    r["nodes"]
                        .As<List<INode>>()
                        .Select(n => FormatNode(n))
                        .ToList())
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

            if (plan == null)
                return null;

            FormatPlan(plan, planType);

            return new Response(plan.ToArray(), markers);
        }

        private static string GetQuery(PlanType planType)
        {
            switch (planType)
            {
                case PlanType.Direct:
                    return DirectQuery;
                case PlanType.OneSwitchNoWalking:
                    return OneSwitchNoWalkingQuery;
                case PlanType.OneSwitchLessThen100Meters:
                    return OneSwitchLessThen100MetersQuery;
                default:
                    throw new NotImplementedException();
            }
        }

        private static string FormatNode(INode n)
        {
            switch (n.Labels[0])
            {
                case "Stop":
                    return "תחנת " + n.Properties["name"].ToString();
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

        private static void FormatPlan(List<string> plan, PlanType planType)
        {
            switch (planType)
            {
                case PlanType.Direct:
                    plan.Insert(3, "עלה ב");
                    plan.Insert(6, "רד ב");
                    break;
                case PlanType.OneSwitchNoWalking:
                    plan.Insert(3, "עלה ב");
                    plan.Insert(6, "רד ב");
                    plan.Insert(12, "עלה ב");
                    plan.Insert(15, "רד ב");
                    break;
                case PlanType.OneSwitchLessThen100Meters:
                    plan.Insert(3, "עלה ב");
                    plan.Insert(6, "רד ב");
                    plan.Insert(9, "לך אל");
                    plan.Insert(13, "עלה ב");
                    plan.Insert(16, "רד ב");
                    break;
                default:
                    throw new NotImplementedException();
            }
        }
    }
}
