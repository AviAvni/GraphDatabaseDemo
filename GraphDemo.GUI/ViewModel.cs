using Google.Maps;
using Google.Maps.StaticMaps;
using System;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace GraphDemo.GUI
{
    public class ViewModel : INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler PropertyChanged;

        private void OnPropertyChanged([CallerMemberName]string name = null) =>
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));

        private string sourceCity;

        public string SourceCity
        {
            get { return sourceCity; }
            set
            {
                sourceCity = value;
                SourceStops = Service.GetStops(sourceCity, sourceStreet, isSourceTrain);
            }
        }

        private string sourceStreet;

        public string SourceStreet
        {
            get { return sourceStreet; }
            set
            {
                sourceStreet = value;
                SourceStops = Service.GetStops(sourceCity, sourceStreet, isSourceTrain);
                OnPropertyChanged();
            }
        }

        private bool isSourceTrain;

        public bool IsSourceTrain
        {
            get { return isSourceTrain; }
            set
            {
                isSourceTrain = value;
                if (isSourceTrain)
                    SourceStreet = null;
                SourceStops = Service.GetStops(sourceCity, sourceStreet, isSourceTrain);
                OnPropertyChanged();
            }
        }

        private string targetCity;

        public string TargetCity
        {
            get { return targetCity; }
            set
            {
                targetCity = value;
                TargetStops = Service.GetStops(targetCity, targetStreet, isTargetTrain);
            }
        }

        private string targetStreet;

        public string TargetStreet
        {
            get { return targetStreet; }
            set
            {
                targetStreet = value;
                TargetStops = Service.GetStops(targetCity, targetStreet, isTargetTrain);
                OnPropertyChanged();
            }
        }

        private bool isTargetTrain;

        public bool IsTargetTrain
        {
            get { return isTargetTrain; }
            set
            {
                isTargetTrain = value;
                if (isTargetTrain)
                    TargetStreet = null;
                TargetStops = Service.GetStops(targetCity, targetStreet, isTargetTrain);
                OnPropertyChanged();
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
                var _ = UpdatePlanAsync();
            }
        }

        private Stop selectedTarget;

        public Stop SelectedTarget
        {
            get { return selectedTarget; }
            set
            {
                selectedTarget = value;
                var _ = UpdatePlanAsync();
            }
        }

        private PlanType[] planTypes;

        public PlanType[] PlanTypes
        {
            get { return planTypes; }
            set { planTypes = value; }
        }

        private PlanType selectedPlanType;

        public PlanType SelectedPlanType
        {
            get { return selectedPlanType; }
            set
            {
                selectedPlanType = value;
                var _ = UpdatePlanAsync();
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
                var _ = UpdatePlanAsync();
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

        public LatLng CenterGeoCoordinate { get; set; }

        public ViewModel()
        {
            GoogleSigned.AssignAllServices(new GoogleSigned(Environment.GetEnvironmentVariable("GoogleApiKey")));
            Times = Enumerable.Range(0, 24).Select(i => i.ToString("00")).ToArray();
            PlanTypes = new[] { PlanType.Direct, PlanType.OneSwitchNoWalking, PlanType.OneSwitchLessThen100Meters };
        }

        private async Task UpdatePlanAsync()
        {
            var response = await Service.CreatePlanAsync(selectedSource?.Id, selectedTarget?.Id, selectedTime, selectedPlanType);
            Plan = response?.Plan;
            UpdateMap(response?.Markers);
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
                Path = new Path()
            };

            foreach (var point in locations)
            {
                map.Path.Points.Add(point);
                map.Markers.Add(point);
            }

            MapUri = map.ToUriSigned().ToString();
        }
    }
}
