using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace app.Entity
{
   public class dataInfo
    {
        int m_infoid;
        string m_linkid;
        string m_uppacket;
        string m_downpacket;
        string m_downnotifypacket;
        string m_successnotifypacket;
        string m_transactionid;
        string m_mobile;
        string m_area;
        decimal m_price;
        int m_pid;
        int m_cid;
        int m_classID;
        string m_state;
        string m_datatime;
        string m_table;

        public dataInfo() { }

        public string Mobile
        {
            get { return m_mobile; }
            set { m_mobile = value; }
        }


        public int Infoid
        {
            get { return m_infoid; }
            set { m_infoid = value; }
        }

        public string LinkId
        {
            get { return m_linkid; }
            set { m_linkid = value; }
        }

        public string upPacket
        {
            get { return m_uppacket; }
            set { m_uppacket = value; }
        }

        public string downPacket
        {
            get { return m_downpacket; }
            set { m_downpacket = value; }
        }


        public string downNotifyPacket
        {
            get { return m_downnotifypacket; }
            set { m_downnotifypacket = value; }
        }


        public string successNotifyPacket
        {
            get { return m_successnotifypacket; }
            set { m_successnotifypacket = value; }
        }


        public string transActionID
        {
            get { return m_transactionid; }
            set { m_transactionid = value; }
        }

        public string Area
        {
            get { return m_area; }
            set { m_area = value; }
        }



        public decimal Price
        {
            get { return m_price; }
            set { m_price = value; }
        }


        public int PID
        {
            get { return m_pid; }
            set { m_pid = value; }
        }


        public int CID
        {
            get { return m_cid; }
            set { m_cid = value; }
        }


        public int ClassID
        {
            get { return m_classID; }
            set { m_classID = value; }
        }


        public string State
        {
            get { return m_state; }
            set { m_state = value; }
        }


        public string DataTime
        {
            get { return m_datatime; }
            set { m_datatime = value; }
        }


        public string Table
        {
            get { return m_table; }
            set { m_table = value; }
        }

    }
}
