using CsvHelper;
using CsvHelper.Configuration;
using Neo4j.Driver;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace GraphDemo
{
    class Program
    {
        const string BasePath = @"C:\Graph\israel-public-transportation";
        static IDriver driver;

        static async Task Main(string[] args)
        {
            driver = GraphDatabase.Driver("bolt://localhost", AuthTokens.Basic("neo4j", "123456"));

            await CreateSchema();

            await Save("agency.txt",
                 "UNWIND $data as item CREATE (:Agency {id: toInteger(item.agency_id), name: item.agency_name, url: item.agency_url, timezone: item.agency_timezone, lang: item.agency_lang})",
                 5000,
                 new Property("agency_id"),
                 new Property("agency_name"),
                 new Property("agency_url"),
                 new Property("agency_timezone"),
                 new Property("agency_lang"));

            await Save("routes.txt",
                  "UNWIND $data as item MATCH (a:Agency {id: toInteger(item.agency_id)}) CREATE (a)-[:OPERATES]->(r:Route {id: item.route_id, desc: item.route_desc, color: item.route_color, short_name: item.route_short_name, long_name: item.route_long_name, type: toInteger(item.route_type)})",
                  5000,
                  new Property("route_id"),
                  new Property("agency_id"),
                  new Property("route_short_name"),
                  new Property("route_long_name"),
                  new Property("route_desc"),
                  new Property("route_type"),
                  new Property("route_color"));

            await Save("trips.txt",
                 "UNWIND $data as item MATCH (r:Route {id: item.route_id}) CREATE (r)<-[:USES]-(t:Trip {id: item.trip_id, service_id: item.service_id, headsign: item.trip_headsign, direction_id: item.direction_id, shape_id: item.shape_id})",
                 5000,
                 new Property("route_id"),
                 new Property("service_id"),
                 new Property("trip_id"),
                 new Property("trip_headsign"),
                 new Property("direction_id"),
                 new Property("shape_id"));

            await Save("stops.txt",
                 "UNWIND $data as item CREATE (s:Stop {id: item.stop_id, name: item.stop_name, location: point({longitude: toFloat(item.stop_lon), latitude: toFloat(item.stop_lat), crs: 'wgs-84'}), parent_station: item.parent_station, location_type: item.location_type, code: item.stop_code, desc: item.stop_desc, zone: item.zone_id})",
                 5000,
                 new Property("stop_id"),
                 new Property("stop_code"),
                 new Property("stop_name"),
                 new Property("stop_desc"),
                 new Property("stop_lat"),
                 new Property("stop_lon"),
                 new Property("location_type"),
                 new Property("parent_station"),
                 new Property("zone_id"));

            await Save("stops.txt",
                 "UNWIND $data as item MATCH (ps:Stop {id: item.parent_station}), (s:Stop {id: item.stop_id}) CREATE (ps)<-[:PART_OF]-(s)",
                 5000,
                 new Property("stop_id"),
                 new Property("parent_station", o => o != null));

            await Save("stop_times.txt",
                 "UNWIND $data as item MATCH (t:Trip {id: item.trip_id}), (s:Stop {id: item.stop_id}) CREATE (t)<-[:PART_OF_TRIP]-(st:Stoptime {arrival_time: localtime(item.arrival_time), departure_time: localtime(item.departure_time), stop_sequence: toInteger(item.stop_sequence), pickup_type: item.pickup_type, drop_off_type: item.drop_off_type, shape_dist_traveled: item.shape_dist_traveled})-[:LOCATED_AT]->(s)",
                 10000,
                 new Property("trip_id"),
                 new Property("arrival_time", selector: FormatTime),
                 new Property("departure_time", selector: FormatTime),
                 new Property("stop_id"),
                 new Property("stop_sequence"),
                 new Property("pickup_type"),
                 new Property("drop_off_type"),
                 new Property("shape_dist_traveled"));

            await Save("trips.txt",
                 "UNWIND $data as item MATCH (s1:Stoptime)-[:PART_OF_TRIP]->(t:Trip), (s2:Stoptime)-[:PART_OF_TRIP]->(t) WHERE t.id=item.trip_id AND s2.stop_sequence=s1.stop_sequence+1 CREATE (s1)-[:PRECEDES]->(s2)",
                 1000,
                 new Property("trip_id"));
        }

        public static object FormatTime(object o)
        {
            var hour = int.Parse(o.ToString().Split(':')[0]);
            return hour >= 24 ? o.ToString().Replace($"{hour}:", $"{hour - 24:00}:") : o;
        }

        private static async Task CreateSchema()
        {
            var session = driver.AsyncSession();
            await session.RunAsync("CREATE CONSTRAINT ON (a:Agency) ASSERT a.id IS UNIQUE");
            await session.RunAsync("CREATE CONSTRAINT ON (r:Route) ASSERT r.id IS UNIQUE");
            await session.RunAsync("CREATE CONSTRAINT ON (t:Trip) ASSERT t.id IS UNIQUE");
            await session.RunAsync("CREATE CONSTRAINT ON (s:Stop) ASSERT s.id IS UNIQUE");
            await session.RunAsync("CREATE INDEX ON :Trip(service_id)");
            await session.RunAsync("CREATE INDEX ON :Stoptime(stop_sequence)");
            await session.RunAsync("CREATE INDEX ON :Stop(name)");
            await session.RunAsync("CREATE INDEX ON :Stop(location)");
            await session.CloseAsync();
        }

        public static async Task Save(string file,
            string query,
            int batch,
            params Property[] properties)
        {
            var sw = Stopwatch.StartNew();

            var session = driver.AsyncSession();

            await CreateStream(file, properties)
                .Where(record => properties.Where(p => p.Condition != null).All(p => p.Condition(record[p.Name])))
                .Buffer(batch)
                .Do(parameters => SaveBatch(query, parameters, session))
                .Scan(0, (acc, args) => acc + args.Count)
                .ForEachAsync(records => PrintProgress(file, records, sw.Elapsed));

            Console.WriteLine();

            await session.CloseAsync();
        }

        private static async IAsyncEnumerable<Dictionary<string, object>> CreateStream(string file, Property[] properties)
        {
            using var reader = new StreamReader(Path.Combine(BasePath, file));
            using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture) { Delimiter = ",", Mode = CsvMode.NoEscape });

            await csv.ReadAsync();
            csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                var parameters = new Dictionary<string, object>(properties.Length);
                foreach (var property in properties)
                {
                    parameters.Add(property.Name, property.Selector(csv.GetField(property.Name)));
                }
                yield return parameters;
            }
        }

        private static Task SaveBatch(string query, IList<Dictionary<string, object>> parameters, IAsyncSession session)
        {
            return session.WriteTransactionAsync(async transaction =>
            {
                await transaction.RunAsync(query, new Dictionary<string, object>
                {
                    ["data"] = parameters
                });
            });
        }

        private static void PrintProgress(string file, int record, TimeSpan elapsed)
        {
            Console.Write($"{file}: {record}, time: {elapsed}");
            Console.SetCursorPosition(0, Console.CursorTop);
        }
    }
}
