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
                 null,
                 "agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang");

            Save("routes.txt",
                 "MATCH (a:Agency {id: toInteger({agency_id})}) CREATE (a)-[:OPERATES]->(r:Route {id: {route_id}, desc: {route_desc}, color: {route_color}, short_name: {route_short_name}, long_name: {route_long_name}, type: toInteger({route_type})})",
                 5000,
                 null,
                 "route_id", "agency_id", "route_short_name", "route_long_name", "route_desc", "route_type", "route_color");

            Save("trips.txt",
                 "MATCH (r:Route {id: {route_id}}) CREATE (r)<-[:USES]-(t:Trip {id: {trip_id}, service_id: {service_id}, headsign: {trip_headsign}, direction_id: {direction_id}, shape_id: {shape_id}})",
                 5000,
                 null,
                 "route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "shape_id");

            Save("stops.txt",
                 "CREATE (s:Stop {id: {stop_id}, name: {stop_name}, location: point({longitude: toFloat({stop_lon}), latitude: toFloat({stop_lat}), crs: 'wgs-84'}), parent_station: {parent_station}, location_type: {location_type}, code: {stop_code}, desc: {stop_desc}, zone: {zone_id}})",
                 5000,
                 null,
                 "stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon", "location_type", "parent_station", "zone_id");

            Save("stops.txt",
                 "MATCH (ps:Stop {id: {parent_station}}), (s:Stop {id: {stop_id}}) CREATE (ps)<-[:PART_OF]-(s)",
                 5000,
                 parameters => parameters["parent_station"] != null,
                 "stop_id", "parent_station");

            Save("stop_times.txt",
                 "MATCH (t:Trip {id: {trip_id}}), (s:Stop {id: {stop_id}}) CREATE (t)<-[:PART_OF_TRIP]-(st:Stoptime {arrival_time: localtime({arrival_time}), departure_time: localtime({departure_time}), stop_sequence: toInteger({stop_sequence}), pickup_type: {pickup_type}, drop_off_type: {drop_off_type}, shape_dist_traveled: {shape_dist_traveled}})-[:LOCATED_AT]->(s)",
                 5000,
                 null,
                 "trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "pickup_type", "drop_off_type", "shape_dist_traveled");

            Save("trips.txt",
                 "MATCH (s1:Stoptime)-[:PART_OF_TRIP]->(t:Trip), (s2:Stoptime)-[:PART_OF_TRIP]->(t) WHERE t.id={trip_id} AND s2.stop_sequence=s1.stop_sequence+1 CREATE (s1)-[:PRECEDES]->(s2)",
                 1000,
                 null,
                 "trip_id");
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
            Func<Dictionary<string, object>, bool> condition,
            params string[] fields)
        {
            var sw = Stopwatch.StartNew();

            Validate(query, fields);

            using (var session = driver.Session())
            {
                Observable
                    .Create<CsvReader>(obs => CreateStream(file, obs))
                    .Select(csv => ReadRecord(fields, csv))
                    .Where(record => condition == null || condition(record))
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

        private static Dictionary<string, object> ReadRecord(string[] fields, CsvReader csv)
        {
            var parameters = new Dictionary<string, object>(fields.Length);
            foreach (var field in fields)
            {
                parameters.Add(field, csv.GetField(field));
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
