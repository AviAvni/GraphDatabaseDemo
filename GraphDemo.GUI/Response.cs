using Google.Maps;

namespace GraphDemo.GUI
{
    public class Response
    {
        public string[] Plan { get; set; }
        public LatLng[] Markers { get; set; }

        public Response(string[] plan, LatLng[] markers)
        {
            Plan = plan;
            Markers = markers;
        }
    }
}
