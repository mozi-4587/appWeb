using System;
using System.Collections.Generic;
using System.Text;

namespace app.Entity
{
    public class treemanageModel
    {
        public treemanageModel() { }

	    private int m_infoidm;
        public int infoidm
	    {
            get { return m_infoidm; }
            set { m_infoidm = value; }
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

        private int m_childnode;
        public int childnode
        {
            get { return m_childnode; }
            set { m_childnode = value; }
        }
        
      

       
    }
}
