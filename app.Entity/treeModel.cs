using System;
using System.Collections.Generic;
using System.Text;

namespace app.Entity
{
    public class treeModel
    {
        public treeModel() { }

	    private int m_infoid;
	    public int infoid
	    {
            get { return m_infoid; }
            set { m_infoid = value; }
	    }

        private string m_text;
        public string text
	    {
            get { return m_text; }
            set { m_text = value; }
	    }

        
        public bool leaf
        {
            get {
                if (childnode > 0)
                    return false;
                else
                    return true;
            }
           
        }

        private string m_name;
        public string name
        {
            get { return m_name; }
            set { m_name = value; }
        }

        private int m_childnode;
        public int childnode
        {
            get { return m_childnode; }
            set { m_childnode = value; }
        }
        
        private string m_value;
        public string value
        {
            get { return m_value; }
            set { m_value = value; }
        }


        private int m_length;
        public int length
        {
            get { return m_length; }
            set { m_length = value; }
        }


        private string m_viewpath;
        public string viewpath
        {
            get { return m_viewpath; }
            set { m_viewpath = value; }
        }

       
    }
}
