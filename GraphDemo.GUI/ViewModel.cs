using Neo4j.Driver.V1;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

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


        public ViewModel()
        {
            driver = GraphDatabase.Driver("bolt://localhost", AuthTokens.Basic("neo4j", "123456"));
            Times = Enumerable.Range(0, 24).Select(i => i.ToString("00")).ToArray();
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
WHERE s1.id = {{selectedSource}} AND s2.id = {{selectedTarget}} AND st1.arrival_time STARTS WITH {{selectedTime}}
RETURN [a, r, t, s1, st1, s2, st2] as nodes";

                var notDirectQuery = $@"
MATCH (s1:Stop)<-[:LOCATED_AT]-(st1:Stoptime)-[:PRECEDES*]->(st2:Stoptime)-[:LOCATED_AT]->(s2:Stop)<-[:LOCATED_AT]-(st3:Stoptime)-[:PRECEDES*]->(st4:Stoptime)-[:LOCATED_AT]->(s3:Stop), 
      (st1)-[:PART_OF_TRIP]->(t:Trip)-[:USES]->(r:Route)<-[:OPERATES]-(a:Agency)
WHERE s1.id = {{selectedSource}} AND s3.id = {{selectedTarget}} AND st1.arrival_time STARTS WITH {{selectedTime}} AND substring(st2.arrival_time, 0, 2) = substring(st3.arrival_time, 0, 2) AND st2.arrival_time < st3.arrival_time
RETURN [a, r, t, s1, st1, s2, st2, s2, st3, s3, st4] as nodes";

                var parameters = new Dictionary<string, object>()
                {
                    [nameof(selectedSource)] = selectedSource.Id,
                    [nameof(selectedTarget)] = selectedTarget.Id,
                    [nameof(selectedTime)] = selectedTime
                };

                Plan = RunQuery(session, directQuery, parameters) ?? RunQuery(session, notDirectQuery, parameters);
            }
        }

        private string[] RunQuery(ISession session, string query, Dictionary<string, object> parameters)
        {
            return session.Run(query, parameters)
                .Select(r =>
                    r["nodes"].As<List<INode>>()
                        .Select(n => FormatNode(n)).ToArray())
                .FirstOrDefault();
        }

        private static string FormatNode(INode n)
        {
            switch (n.Labels[0])
            {
                case "Stop":
                    return "תחנה " + n.Properties["desc"].ToString(); ;
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
