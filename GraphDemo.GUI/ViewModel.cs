using Google.Maps;
using Google.Maps.Direction;
using Google.Maps.StaticMaps;
using Neo4j.Driver.V1;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace GraphDemo.GUI
{
    public class Stop
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Desc { get; set; }
    }

    public class ViewModel : INotifyPropertyChanged
    {
        static IDriver driver;

        public event PropertyChangedEventHandler PropertyChanged;

        private void OnPropertyChanged([CallerMemberName]string name = null) =>
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));

        private string source;

        public string Source
        {
            get { return source; }
            set
            {
                source = value;
                SourceStops = GetStops(source);
            }
        }

        private string target;

        public string Target
        {
            get { return target; }
            set
            {
                target = value;
                TargetStops = GetStops(target);
            }
        }

        private Stop[] sourceStops;

        public Stop[] SourceStops
        {
            get { return sourceStops; }
            set
            {
                sourceStops = value;
                OnPropertyChanged();
            }
        }

        private Stop[] targetStops;

        public Stop[] TargetStops
        {
            get { return targetStops; }
            set
            {
                targetStops = value;
                OnPropertyChanged();
            }
        }

        private Stop selectedSource;

        public Stop SelectedSource
        {
            get { return selectedSource; }
            set
            {
                selectedSource = value;
                CreatePlan();
            }
        }

        private Stop selectedTarget;

        public Stop SelectedTarget
        {
            get { return selectedTarget; }
            set
            {
                selectedTarget = value;
                CreatePlan();
            }
        }

        private string[] plan;

        public string[] Plan
        {
            get { return plan; }
            set
            {
                plan = value;
                OnPropertyChanged();
            }
        }

        private string[] times;

        public string[] Times
        {
            get { return times; }
            set
            {
                times = value;
                OnPropertyChanged();
            }
        }

        private string selectedTime;

        public string SelectedTime
        {
            get { return selectedTime; }
            set
            {
                selectedTime = value;
                CreatePlan();
            }
        }

        private string mapUri;

        public string MapUri
        {
            get { return mapUri; }
            set
            {
                mapUri = value;
                OnPropertyChanged();
            }
        }

        private int zoom;

        public int Zoom
        {
            get { return zoom; }
            set { zoom = value; }
        }

        private const double MinLatitude = -90D;
        private const double MaxLatitude = 90D;
        private const double MinLongitude = -180D;
        private const double MaxLongitude = 180D;

        public LatLng CenterGeoCoordinate { get; set; }

        public ViewModel()
        {
            GoogleSigned.AssignAllServices(new GoogleSigned("AIzaSyBBn-zroiSmFlStmyBnhojNQ1zC6b1RC24"));
            driver = GraphDatabase.Driver("bolt://localhost", AuthTokens.Basic("neo4j", "123456"));
            Times = Enumerable.Range(0, 24).Select(i => i.ToString("00")).ToArray();
            Zoom = 8;
        }

        private Stop[] GetStops(string desc)
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

        private void CreatePlan()
        {
            if (selectedSource == null || selectedTarget == null || selectedTime == null)
                return;

            using (var session = driver.Session())
            {
                var directQuery = $@"
MATCH (s1:Stop)<-[:LOCATED_AT]-(st1:Stoptime)-[:PRECEDES*]->(st2:Stoptime)-[:LOCATED_AT]->(s2:Stop),
      (st1)-[:PART_OF_TRIP]->(t:Trip)-[:USES]->(r:Route)<-[:OPERATES]-(a:Agency)
WHERE s1.id = {{selectedSource}} AND 
      s2.id = {{selectedTarget}} AND 
      st1.arrival_time STARTS WITH {{selectedTime}}
RETURN [a, r, t, s1, st1, s2, st2] as nodes";

                var notDirect1Query = $@"
MATCH (s1:Stop)<-[:LOCATED_AT]-(st1:Stoptime)-[:PRECEDES*]->(st2:Stoptime)-[:LOCATED_AT]->(s2:Stop)<-[:LOCATED_AT]-(st3:Stoptime)-[:PRECEDES*]->(st4:Stoptime)-[:LOCATED_AT]->(s3:Stop), 
      (st1)-[:PART_OF_TRIP]->(t:Trip)-[:USES]->(r:Route)<-[:OPERATES]-(a:Agency)
WHERE s1.id = {{selectedSource}} AND 
      s3.id = {{selectedTarget}} AND 
      st1.arrival_time STARTS WITH {{selectedTime}} AND 
      substring(st2.arrival_time, 0, 2) = substring(st3.arrival_time, 0, 2) AND 
      st2.arrival_time < st3.arrival_time
RETURN [a, r, t, s1, st1, s2, st2, s2, st3, s3, st4] as nodes";

                var notDirect2Query = $@"
MATCH (s1:Stop)<-[:LOCATED_AT]-(st1:Stoptime)-[:PRECEDES*]->(st2:Stoptime)-[:LOCATED_AT]->(s2:Stop), 
      (s3:Stop)<-[:LOCATED_AT]-(st3:Stoptime)-[:PRECEDES*]->(st4:Stoptime)-[:LOCATED_AT]->(s4:Stop), 
      (st1)-[:PART_OF_TRIP]->(t:Trip)-[:USES]->(r:Route)<-[:OPERATES]-(a:Agency)
WHERE s1.id = {{selectedSource}} AND 
      s4.id = {{selectedTarget}} AND 
      distance(s2.location, s3.location) < 500 AND
      st1.arrival_time STARTS WITH {{selectedTime}} AND 
      substring(st2.arrival_time, 0, 2) = substring(st3.arrival_time, 0, 2) AND 
      st2.arrival_time < st3.arrival_time
RETURN [a, r, t, s1, st1, s2, st2, s3, st3, s4, st4] as nodes";

                var parameters = new Dictionary<string, object>()
                {
                    [nameof(selectedSource)] = selectedSource.Id,
                    [nameof(selectedTarget)] = selectedTarget.Id,
                    [nameof(selectedTime)] = selectedTime
                };

                Plan = 
                    RunQuery(session, directQuery, parameters) ?? 
                    RunQuery(session, notDirect1Query, parameters) ??
                    RunQuery(session, notDirect2Query, parameters);
            }
        }

        private string[] RunQuery(ISession session, string query, Dictionary<string, object> parameters)
        {
            var result = session.Run(query, parameters);
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
                        .Select(n => n["location"].As<Neo4j.Driver.V1.Point>())
                        .Select(p => new LatLng(p.Y, p.X))
                        .ToArray())
                .FirstOrDefault();

            if (markers != null)
                UpdateMap(markers);

            return plan;
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

        private void UpdateMap(LatLng[] locations)
        {
            CenterGeoCoordinate = 
                new LatLng(locations.Select(l => l.Latitude).Average(), 
                           locations.Select(l => l.Longitude).Average());

            var map = new StaticMapRequest
            {
                Language = "he-IL",
                Center = CenterGeoCoordinate,
                Size = new MapSize(400, 400),
                Zoom = Zoom,
                Path = new Path()
            };

            for (int i = 0; i < locations.Length; i++)
            {
                LatLng point = locations[i];
                map.Path.Points.Add(point);
                map.Markers.Add(point);
            }

            MapUri = map.ToUri().ToString();
        }
    }
}
