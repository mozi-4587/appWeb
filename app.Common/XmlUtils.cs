using System;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using System.Net;
using System.Collections;
using System.IO;
using System.Xml.Serialization;
using System.Collections.ObjectModel;
using Microsoft.VisualBasic;
using System.Text.RegularExpressions;

namespace app.Common
{
    public class XmlUtils
    {
        public static void ReadParseXml(string str,string node)
        {
            XmlDocument xmlDoc = new XmlDocument();
            xmlDoc.Load(str);
            //查找<users>
            XmlNode root = xmlDoc.SelectSingleNode(node);
            //获取到所有<users>的子节点
            XmlNodeList nodeList = root.ChildNodes;
            //遍历所有子节点
            foreach (XmlNode xn in nodeList)
            {
                XmlElement xe = (XmlElement)xn;
                XmlNodeList subList = xe.ChildNodes;
                foreach (XmlNode xmlNode in subList)
                {
                    if ("name".Equals(xmlNode.Name))
                    {
                        Console.WriteLine("姓名：" + xmlNode.InnerText);
                    }
                    else if ("email".Equals(xmlNode.Name))
                    {
                        Console.WriteLine("邮箱：" + xmlNode.InnerText);
                    }
                }
            }
        }


       public static void ReadParseXml2()
        {
            XmlDocument xmlDoc = new XmlDocument();
            xmlDoc.Load("E:/Data/VisualStudio/C#/app001/ConsoleApp/App01/userlist.xml");
            //查找<users>
            XmlNode root = xmlDoc.SelectSingleNode("users");
            //获取到所有<users>的子节点
            XmlNodeList nodeList = xmlDoc.SelectSingleNode("users").ChildNodes;
            //遍历所有子节点
            foreach (XmlNode xn in nodeList)
            {
                XmlElement xe = (XmlElement)xn;
                Console.WriteLine("节点的ID为： " + xe.GetAttribute("id"));
                XmlNodeList subList = xe.ChildNodes;
                foreach (XmlNode xmlNode in subList)
                {
                    Console.WriteLine(xmlNode.InnerText);
                }
            }
        }
    }
}
