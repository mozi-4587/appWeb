using System;
using System.Collections.Generic;
using System.Text;

namespace app.Entity
{
    public class mobileArea
    {
        int m_id;	
        string m_number;
        string m_area;
        string m_type;
        string m_city;
        string m_province;
        string m_areaCode;
        string m_postCode;

        public mobileArea() { }

        public int ID
        {
            get { return m_id; }
            set { m_id = value; }
        }

        public string Number
        {
            get { return m_number; }
            set { m_number = value; }
        }

        public string Area
        {
           
            get {
                if (!string.IsNullOrEmpty(City))
                    return Province + ' ' + City;
                else if (!string.IsNullOrEmpty(Province))
                    return Province;
                else
                    return m_area;
            }
            set { m_area = value; }
        }

        public string Type
        {
            get { return m_type; }
            set { m_type = value; }
        }

        public string AreaCode
        {
            get { return m_areaCode; }
            set { m_areaCode = value; }
        }

        public string PostCode
        {
            get { return m_postCode; }
            set { m_postCode = value; }
        }

        public string City
        {
            get { return m_city; }
            set { m_city = value; }
        }

        public string Province
        {
            get { return m_province; }
            set { m_province = value; }
        }

    }
}
