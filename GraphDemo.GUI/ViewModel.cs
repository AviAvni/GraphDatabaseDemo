using Google.Maps;
using Google.Maps.StaticMaps;
using System;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;

namespace GraphDemo.GUI
{
    public class ViewModel : INotifyPropertyChanged
    {
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
                SourceStops = Service.GetStops(source);
            }
        }

        private string target;

        public string Target
        {
            get { return target; }
            set
            {
                target = value;
                TargetStops = Service.GetStops(target);
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
                var response = Service.CreatePlan(selectedSource.Id, selectedTarget.Id, selectedTime);
                Plan = response.Plan;
                UpdateMap(response.Markers);
            }
        }

        private Stop selectedTarget;

        public Stop SelectedTarget
        {
            get { return selectedTarget; }
            set
            {
                selectedTarget = value;
                var response = Service.CreatePlan(selectedSource.Id, selectedTarget.Id, selectedTime);
                Plan = response.Plan;
                UpdateMap(response.Markers);
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
                var response = Service.CreatePlan(selectedSource.Id, selectedTarget.Id, selectedTime);
                Plan = response.Plan;
                UpdateMap(response.Markers);
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
            GoogleSigned.AssignAllServices(new GoogleSigned(Environment.GetEnvironmentVariable("GoogleApiKey")));
            Times = Enumerable.Range(0, 24).Select(i => i.ToString("00")).ToArray();
            Zoom = 8;
        }

        private void UpdateMap(LatLng[] locations)
        {
            if (locations == null)
                return;

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

            foreach (var point in locations)
            {
                map.Path.Points.Add(point);
                map.Markers.Add(point);
            }

            MapUri = map.ToUri().ToString();
        }
    }
}
