using CsvHelper;
using Neo4j.Driver.V1;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace GraphDemo
{
    class Program
    {
        const string BasePath = @"C:\israel-public-transportation";
        static IDriver driver;

        static void Main(string[] args)
        {
            driver = GraphDatabase.Driver("bolt://localhost", AuthTokens.Basic("neo4j", "123456"));

            CreateSchema();

            Save("agency.txt",
                 "CREATE (:Agency {id: toInteger({agency_id}), name: {agency_name}, url: {agency_url}, timezone: {agency_timezone}, lang: {agency_lang}})",
                 5000,
                 new Property("agency_id"),
                 new Property("agency_name"), 
                 new Property("agency_url"), 
                 new Property("agency_timezone"),
                 new Property("agency_lang"));

            Save("routes.txt",
                 "MATCH (a:Agency {id: toInteger({agency_id})}) CREATE (a)-[:OPERATES]->(r:Route {id: {route_id}, desc: {route_desc}, color: {route_color}, short_name: {route_short_name}, long_name: {route_long_name}, type: toInteger({route_type})})",
                 5000,
                 new Property("route_id"), 
                 new Property("agency_id"), 
                 new Property("route_short_name"), 
                 new Property("route_long_name"), 
                 new Property("route_desc"), 
                 new Property("route_type"),
                 new Property("route_color"));

            Save("trips.txt",
                 "MATCH (r:Route {id: {route_id}}) CREATE (r)<-[:USES]-(t:Trip {id: {trip_id}, service_id: {service_id}, headsign: {trip_headsign}, direction_id: {direction_id}, shape_id: {shape_id}})",
                 5000,
                 new Property("route_id"), 
                 new Property("service_id"), 
                 new Property("trip_id"), 
                 new Property("trip_headsign"), 
                 new Property("direction_id"),
                 new Property("shape_id"));

            Save("stops.txt",
                 "CREATE (s:Stop {id: {stop_id}, name: {stop_name}, location: point({longitude: toFloat({stop_lon}), latitude: toFloat({stop_lat}), crs: 'wgs-84'}), parent_station: {parent_station}, location_type: {location_type}, code: {stop_code}, desc: {stop_desc}, zone: {zone_id}})",
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

            Save("stops.txt",
                 "MATCH (ps:Stop {id: {parent_station}}), (s:Stop {id: {stop_id}}) CREATE (ps)<-[:PART_OF]-(s)",
                 5000,
                 new Property("stop_id"),
                 new Property("parent_station", o => o != null));

            Save("stop_times.txt",
                 "MATCH (t:Trip {id: {trip_id}}), (s:Stop {id: {stop_id}}) CREATE (t)<-[:PART_OF_TRIP]-(st:Stoptime {arrival_time: localtime({arrival_time}), departure_time: localtime({departure_time}), stop_sequence: toInteger({stop_sequence}), pickup_type: {pickup_type}, drop_off_type: {drop_off_type}, shape_dist_traveled: {shape_dist_traveled}})-[:LOCATED_AT]->(s)",
                 10000,
                 new Property("trip_id"), 
                 new Property("arrival_time", selector: FormatTime), 
                 new Property("departure_time", selector: FormatTime), 
                 new Property("stop_id"), 
                 new Property("stop_sequence"), 
                 new Property("pickup_type"), 
                 new Property("drop_off_type"),
                 new Property("shape_dist_traveled"));

            Save("trips.txt",
                 "MATCH (s1:Stoptime)-[:PART_OF_TRIP]->(t:Trip), (s2:Stoptime)-[:PART_OF_TRIP]->(t) WHERE t.id={trip_id} AND s2.stop_sequence=s1.stop_sequence+1 CREATE (s1)-[:PRECEDES]->(s2)",
                 100,
                 new Property("trip_id"));
        }

        public static object FormatTime(object o)
        {
            var hour = int.Parse(o.ToString().Split(':')[0]);
            return hour >= 24 ? o.ToString().Replace($"{hour}:", $"{(hour - 24).ToString("00")}:") : o;
        }

        private static void CreateSchema()
        {
            using (var session = driver.Session())
            {
                session.Run("CREATE CONSTRAINT ON (a:Agency) ASSERT a.id IS UNIQUE");
                session.Run("CREATE CONSTRAINT ON (r:Route) ASSERT r.id IS UNIQUE");
                session.Run("CREATE CONSTRAINT ON (t:Trip) ASSERT t.id IS UNIQUE");
                session.Run("CREATE CONSTRAINT ON (s:Stop) ASSERT s.id IS UNIQUE");
                session.Run("CREATE INDEX ON :Trip(service_id)");
                session.Run("CREATE INDEX ON :Stoptime(stop_sequence)");
                session.Run("CREATE INDEX ON :Stop(name)");
                session.Run("CREATE INDEX ON :Stop(location)");
            }
        }

        public static void Save(string file,
            string query,
            int batch,
            params Property[] properties)
        {
            var sw = Stopwatch.StartNew();

            Validate(query, properties.Select(p => p.Name).ToArray());

            using (var session = driver.Session())
            {
                Observable
                    .Create<CsvReader>(obs => CreateStream(file, obs))
                    .Select(csv => ReadRecord(properties, csv))
                    .Where(record => properties.Where(p => p.Condition != null).All(p => p.Condition(record[p.Name])))
                    .Buffer(batch)
                    .Do(parameters => SaveBatch(query, parameters, session))
                    .Scan(0, (acc, args) => acc + args.Count)
                    .Do(records => PrintProgress(file, records, sw.Elapsed))
                    .Wait();

                Console.WriteLine();
            }
        }

        private static void Validate(string query, string[] fields)
        {
            var ps = Regex.Matches(query, "{([^:]*?)}").OfType<Match>().Select(x => x.Groups[1].Value).ToList();
            if (ps.Except(fields).Any()) throw new Exception(string.Join(", ", ps.Except(fields)));
            if (fields.Except(ps).Any()) throw new Exception(string.Join(", ", fields.Except(ps)));
        }

        private static IDisposable CreateStream(string file, IObserver<CsvReader> obs)
        {
            using (var reader = new StreamReader(Path.Combine(BasePath, file)))
            using (var csv = new CsvReader(reader))
            {
                csv.Configuration.IgnoreQuotes = true;
                csv.Read();
                csv.ReadHeader();

                while (csv.Read())
                {
                    obs.OnNext(csv);
                }

                obs.OnCompleted();
            }

            return Disposable.Empty;
        }

        private static Dictionary<string, object> ReadRecord(Property[] properties, CsvReader csv)
        {
            var parameters = new Dictionary<string, object>(properties.Length);
            foreach (var property in properties)
            {
                parameters.Add(property.Name, property.Selector(csv.GetField(property.Name)));
            }

            return parameters;
        }

        private static void SaveBatch(string query, IList<Dictionary<string, object>> parameters, ISession session)
        {
            session.WriteTransaction(transaction =>
            {
                foreach (var item in parameters)
                {
                    transaction.Run(query, item);
                }
            });
        }

        private static void PrintProgress(string file, int record, TimeSpan elapsed)
        {
            Console.Write($"{file}: {record}, time: {elapsed}");
            Console.SetCursorPosition(0, Console.CursorTop);
        }
    }
}
