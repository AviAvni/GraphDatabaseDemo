using System;

namespace GraphDemo
{
    public class Property
    {
        public string Name { get; set; }
        public Func<object, bool> Condition { get; set; }
        public Func<object, object> Selector { get; set; }

        public Property(string name, Func<object, bool> condition, Func<object, object> selector)
        {
            Name = name;
            Condition = condition;
            Selector = selector;
        }

        public Property(string name, Func<object, bool> condition)
            : this(name, condition, o => o)
        {

        }

        public Property(string name, Func<object, object> selector)
            : this(name, o => true, selector)
        {

        }

        public Property(string name)
            : this(name, o => true, o => o)
        {

        }
    }
}
