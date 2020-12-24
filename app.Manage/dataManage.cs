using System;
using System.Collections.Generic;
using System.Configuration;
using System.Text;
using System.Data;
using System.Data.SqlClient;

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;

using app.Common;
using app.Entity;
using app.Data;
using app.Cache;
using System.IO;
using System.Diagnostics;


namespace app.Manage
{
    public class dataManage
    {


        /// <summary>
        /// 获取目录数据
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public static string getDirectory(int id)
        {

            string sql = "select a.infoid,a.name,a.viewpath,LEN(a.sqlcommand) as length,(select COUNT(infoid) from directory as b where a.infoid=b.pid) as childnode  from directory as a where pid=" + id + " order by sort asc";
            
            DataTable dt=SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction,CommandType.Text,sql).Tables[0];
            
                IList<treeModel> model = new List<treeModel>();
                foreach (DataRow row in dt.Rows)
                {
                    treeModel item = new treeModel();
                    item.infoid = Convert.ToInt32(row["infoid"]);
                    item.text = row["name"].ToString();
                    item.name = row["name"].ToString();
                    item.childnode = Utils.StrToInt(row["childnode"], 0);
                    item.length = Utils.StrToInt(row["length"], 0);
                    item.viewpath = row["viewpath"].ToString();
                    model.Add(item);
                }
                return JsonConvert.SerializeObject(model);

        }


        /// <summary>
        /// 获取统计数据
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public static string getStatistics(int id)
        {
            string info = "data:[]";
     
            string sql = "SELECT sqlcommand FROM directory where infoid="+ id;
            object obj = SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            if (obj != System.DBNull.Value)
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.StoredProcedure, Convert.ToString(obj)).Tables[0];

                if (dt.Rows.Count > 0)
                  
                info ="{data:"+JsonConvert.SerializeObject(dt, new DataTableConverter())+"}";
            }

            return info;
           

        }


        //电信点播包月统计查询
        public static string getStatistics(string search)
        {
            string info = null;
            string sql = null;
            string param="";
          
            JObject datajson = JObject.Parse(search);
            string stime = datajson["stime"].ToString();
            string etime = datajson["etime"].ToString();

            if (stime.Length < 5)
                datajson["stime"] = "0" + stime;
            if (etime.Length < 5)
                datajson["etime"] = "0" + etime;

            if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
            {

                string eday = datajson["edate"].ToString();
                string[] temp = eday.Split('-');
                if (temp[1].Length < 2)
                    temp[1] = "0" + temp[1];
                if (temp[2].Length < 2)
                    temp[2] = "0" + temp[2];
                datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];
                param += " and a.datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"] + "' and  DATEADD(MINUTE,+1,'" + datajson["edate"].ToString() + " " + datajson["etime"] + "')";

            }
            if (!string.IsNullOrEmpty(datajson["company"].ToString()))
            {
                if (Convert.ToInt32(datajson["company"]) > 0)
                    param += " and a.companyID=" + datajson["company"].ToString();
            }

            if (Convert.ToInt32(datajson["index"]) == 1)
            {
                 
                   sql="select CONVERT(varchar(10),a.datatime,120) AS 日期,"
	                 +"sum(case when a.orderresult='定购' and a.resultCode=0 and a.status='成功' then 1 else 0 end) as 上行总量,"
	                 +"COUNT(DISTINCT(case when  a.resultCode=0 and  a.orderresult='定购' and a.status='成功' then a.mobile end)) as 独立用户,"
                     +"case when count(d.infoid)>0 then count(d.infoid) else 0 end as 成功定购总量,"
	                 +"sum(case when a.resultCode=0 and a.orderresult='退订' then 1 else 0 end) as 退订总量,"
                     +"COUNT(d.infoid)-sum(case when a.resultCode=0 and a.orderresult='退订' then 1 else 0 end) as 实际定购,"
                     +"sum(case when d.infoid>0 then a.fee else 0 end)-sum(case when a.resultCode=0 and a.orderresult='退订' then a.fee else 0 end) as  有效信息费,"
                     +"case when COUNT(d.infoid)-sum(case when a.resultCode=0 and a.orderresult='退订' then 1 else 0 end)>0 then  Convert(decimal(18,2),Convert(decimal(18,2),COUNT(d.infoid)-sum(case when a.resultCode=0 and a.orderresult='退订' then 1 else 0 end))/Convert(decimal(18,2),count(d.infoid))*100) else 0 end 转化率,"
	                 +"sum(case when c.resultCode=0 and c.OPType=0 then 1 else 0 end) as 同步成功总量,"
                     + "case when sum(case when c.resultCode=0 and c.OPType=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.resultCode=0 and c.OPType=0 then 1 else 0 end))/Convert(decimal(18,2),(COUNT(d.infoid))))*100 else 0 end as 同步率,"
                     +"sum(case when c.resultCode=0 and c.OPType=0 then c.fee end) as 同步有效信息费,"
                     +"sum(case when c.resultCode=0 and c.OPType=0 and c.result=1 then 1 else 0 end) as 回掉失败"
                     + " from tab_2016 as a  WITH(NOLOCK) left join record_info as d on d.infoid=a.infoid left join tab_notify as c on a.infoid=c.infoid where a.buyID=2" + param + " group by convert(varchar(10), a.datatime,120) order by convert(varchar(10), a.datatime,120) desc";
           
             
            }
            else if (Convert.ToInt32(datajson["index"]) == 2)
            {

                sql = "select a.name as 省份,"
                   + "sum(case when b.buyID=2 and b.orderresult='定购' and b.status='成功'" + param.Replace("a.", "b.") + " then 1 else 0 end) as 上行总量,"
                   + "COUNT(DISTINCT(case when b.buyID=2 and b.resultCode=0" + param.Replace("a.", "b.") + " then b.mobile end)) as 独立用户,"
                   + "sum(case when b.buyID=2 and b.orderresult='定购' and b.status='成功'" + param.Replace("a.", "b.") + " then 1 else 0 end) as 成功定购总量,"
                   + "sum(case when b.buyID=2 and b.orderresult='退订' and b.resultCode=0" + param.Replace("a.", "b.") + "  then 1 else 0 end) as 退订总量,"
                   + "sum(case when b.buyID=2 and b.orderresult='定购' and b.status='成功'" + param.Replace("a.", "b.") + "  then 1 else 0 end)- sum(case when b.buyID=2 and b.orderresult='退订' and b.resultCode=0" + param.Replace("a.", "b.") + " then 1 else 0 end) as 实际定购,"
                   + "sum(case when b.buyID=2 and b.orderresult='定购' and b.status='成功'" + param.Replace("a.", "b.") + "  then b.fee else 0 end)- sum(case when b.buyID=2 and b.orderresult='退订' and b.resultCode=0" + param.Replace("a.", "b.") + " then b.fee else 0 end) as 有效信息费,"
                   + "case when (sum(case when b.buyID=2 and b.orderresult='定购' and b.status='成功' and b.resultCode=0" + param.Replace("a.", "b.") + " then 1 else 0 end)- sum(case when b.buyID=2 and b.resultCode=0 and b.orderresult='退订'" + param.Replace("a.", "b.") + " then 1 else 0 end))>0 then Convert(decimal(18,2),(Convert(decimal(18,2),sum(case when b.buyID=2 and b.orderresult='定购' and b.status='成功' and b.resultCode=0" + param.Replace("a.", "b.") + " then 1 else 0 end)- sum(case when b.buyID=2 and b.resultCode=0 and b.orderresult='退订'" + param.Replace("a.", "b.") + " then 1 else 0 end)))/Convert(decimal(18,2),sum(case when b.buyID=2 and b.orderresult='定购'and status='成功' and b.resultCode=0" + param.Replace("a.", "b.") + " then 1 else 0 end)))*100 else 0 end as 转化率,"
                   + "sum(case when c.buyID=2 and c.resultCode=0 and c.OPType=0" + param.Replace("a.", "c.") + " then 1 else 0 end) as 同步成功总量,"
                   + "case when sum(case when c.buyID=2 and c.resultCode=0 and c.OPType=0" + param.Replace("a.", "c.") + " then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),COUNT(DISTINCT(case when b.buyID=2 and b.resultCode=0" + param.Replace("a.", "b.") + " then b.mobile end)))/Convert(decimal(18,2),sum(case when b.buyID=2 and b.resultCode=0 and b.orderresult='定购' and b.status='成功'" + param.Replace("a.", "b.") + " then 1 else 0 end))*100) else 0 end as 同步率,"
                   + "sum(case when c.buyID=2 and c.resultCode=0 and c.OPType=0" + param.Replace("a.", "c.") + " then c.fee else 0 end) as 同步有效信息费,"
                   + "sum(case when c.buyID=2 and c.resultCode=0 and c.OPType=0 and c.result=1" + param.Replace("a.", "c.") + " then 1 else 0 end) as 回掉失败"
                   + " from areaInfo as a left join tab_2016 as b on SUBSTRING(b.area,0,CHARINDEX(' ',area))=a.name left join tab_notify as c on c.infoid=b.infoid where a.parent_id=0 group by a.name order by name";
            }
            else
            {
                sql = "select e.name as 公司名称,a.fee as 资费,"
                     + "sum(case when a.orderresult='定购' and a.resultCode=0 and a.status='成功' then 1 else 0 end) as 上行总量,"
                     + "COUNT(DISTINCT(case when  a.resultCode=0 and  a.orderresult='定购' and a.status='成功' then a.mobile end)) as 独立用户,"
                     + "case when count(d.infoid)>0 then count(d.infoid) else 0 end as 成功定购总量,"
                     + "sum(case when a.resultCode=0 and a.orderresult='退订' then 1 else 0 end) as 退订总量,"
                     + "COUNT(d.infoid)-sum(case when a.resultCode=0 and a.orderresult='退订' then 1 else 0 end) as 实际定购,"
                     + "sum(case when d.infoid>0 then a.fee else 0 end)-sum(case when a.resultCode=0 and a.orderresult='退订' then a.fee else 0 end) as  有效信息费,"
                     + "case when COUNT(d.infoid)-sum(case when a.resultCode=0 and a.orderresult='退订' then 1 else 0 end)>0 then  Convert(decimal(18,2),Convert(decimal(18,2),COUNT(d.infoid)-sum(case when a.resultCode=0 and a.orderresult='退订' then 1 else 0 end))/Convert(decimal(18,2),count(d.infoid))*100) else 0 end 转化率,"
                     + "sum(case when c.resultCode=0 and c.OPType=0 then 1 else 0 end) as 同步成功总量,"
                     + "case when sum(case when c.resultCode=0 and c.OPType=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.resultCode=0 and c.OPType=0 then 1 else 0 end))/Convert(decimal(18,2),(COUNT(d.infoid))))*100 else 0 end as 同步率,"
                     + "sum(case when c.resultCode=0 and c.OPType=0 then c.fee end) as 同步有效信息费,"
                     + "sum(case when c.resultCode=0 and c.OPType=0 and c.result=1 then 1 else 0 end) as 回掉失败"
                     + " from tab_2016 as a WITH(NOLOCK) left join record_info as d on d.infoid=a.infoid left join tab_notify as c on a.infoid=c.infoid right join company as e on a.companyid=e.infoid where a.buyID=2" + param
                     + " group by e.name,a.fee order by e.name";
                     
            }

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)

                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";

            return info;

        }



      


        public static string getStatisticsPay(string search)
        {
            string info = null;
            string sql = null;
            string param = "";
            string times = "";
            string ptimes = "";
            string company = "";
            JObject datajson = JObject.Parse(search);
            string stime = datajson["stime"].ToString();
            string etime = datajson["etime"].ToString();

            if (stime.Length < 5)
                datajson["stime"] = "0" + stime;
            if (etime.Length < 5)
                datajson["etime"] = "0" + etime;


            if (Convert.ToInt32(datajson["index"]) == 1)
            {
                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                    DateTime t = DateTime.Parse(datajson["edate"].ToString());
                    DateTime t1 = t.AddDays(1);
                    string eday = t1.ToShortDateString();
                    string[] temp = eday.Split('/');
                    if (temp[1].Length < 2)
                        temp[1] = "0" + temp[1];
                    if (temp[2].Length < 2)
                        temp[2] = "0" + temp[2];
                    datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];
                    param += " where a.datatime between '" + datajson["sdate"].ToString() + "' and '" + datajson["edate"].ToString() + "'";
                }
                if (!string.IsNullOrEmpty(datajson["company"].ToString()))
                {
                    if (Convert.ToInt32(datajson["company"]) > 0)
                    {
                        company = "b.companyID=" + datajson["company"].ToString() + " and ";
                        if (param.Length > 0)
                            param += " and a.companyID=" + datajson["company"].ToString();
                        else
                            param += " where a.companyID=" + datajson["company"].ToString();
                    }
                }

                 sql = " select convert(varchar(10), a.datatime,120) as 日期,"
                      + "(select count(b.infoid) from spdataInfo_2016 as b where " + company + " convert(varchar(10), b.datatime ,120)=convert(varchar(10), a.datatime,120) and b.ordertype=1) as 上行总量,"
                      + "(select count(b.infoid) from spdataInfo_2016 as b where " + company + " b.datatime between convert(varchar(10), a.datatime,120)+' " + datajson["stime"].ToString() + "' and convert(varchar(10), a.datatime,120)+' " + datajson["etime"].ToString() + "' and b.ordertype=1 and b.orderStatus=1) as 成功订购总量,"
                      + "(select CASE WHEN sum(orderfee) IS NULL THEN 0 ELSE sum(orderfee) END from spdataInfo_2016 as b where " + company + " b.datatime between convert(varchar(10), a.datatime,120)+' " + datajson["stime"].ToString() + "' and convert(varchar(10), a.datatime,120)+' " + datajson["etime"].ToString() + "' and b.ordertype=1 and b.orderStatus=1) as 有效信息费,"
                      + "Convert(decimal(18,2),(CASE WHEN (select count(b.infoid) from spdataInfo_2016 as b where " + company + " b.datatime between convert(varchar(10), a.datatime,120)+' " + datajson["stime"].ToString() + "' and convert(varchar(10), a.datatime,120)+' " + datajson["etime"].ToString() + "' and b.ordertype=1 and b.orderStatus=1)>0 THEN (select Convert(decimal(18,2),count(b.infoid)) from spdataInfo_2016 as b where " + company + " b.datatime between convert(varchar(10), a.datatime,120)+' " + datajson["stime"].ToString() + "' and convert(varchar(10), a.datatime,120)+' " + datajson["etime"].ToString() + "' and b.ordertype=1 and b.orderStatus=1)/(select Convert(decimal(18,2),count(b.infoid)) from spdataInfo_2016 as b where " + company + " b.datatime between convert(varchar(10), a.datatime,120)+' " + datajson["stime"].ToString() + "' and convert(varchar(10), a.datatime,120)+' " + datajson["etime"].ToString() + "' and b.ordertype=1)*100 END)) as 转化率,"
                      + "(select count(b.infoid) from tab_sync as b where " + company + " b.orderStatus=1 and  b.datatime between convert(varchar(10), a.datatime,120)+' " + datajson["stime"].ToString() + "' and convert(varchar(10), a.datatime,120)+' " + datajson["etime"].ToString() + "') as 同步成功总量,"
                      + "Convert(decimal(18,2),(CASE WHEN (select count(b.infoid) from tab_sync as b where " + company + " b.orderStatus=1 and b.datatime between convert(varchar(10), a.datatime,120)+' " + datajson["stime"].ToString() + "' and convert(varchar(10), a.datatime,120)+' " + datajson["etime"].ToString() + "')>0 THEN (select Convert(decimal(18,2),count(b.infoid)) from tab_sync as b where " + company + " b.orderStatus=1 and b.datatime between convert(varchar(10), a.datatime,120)+' " + datajson["stime"].ToString() + "' and convert(varchar(10), a.datatime,120)+' " + datajson["etime"].ToString() + "')/(select Convert(decimal(18,2),count(b.infoid)) from spdataInfo_2016 as b where " + company + " b.datatime between convert(varchar(10), a.datatime,120)+' " + datajson["stime"].ToString() + "' and convert(varchar(10), a.datatime,120)+' " + datajson["etime"].ToString() + "' and b.ordertype=1 and b.orderStatus=1)*100 END)) as 同步率,"
                      + "(select CASE WHEN sum(orderfee) IS NULL THEN 0 ELSE sum(orderfee) END from tab_sync as b where " + company + " b.datatime between convert(varchar(10), a.datatime,120)+' " + datajson["stime"].ToString() + "' and convert(varchar(10), a.datatime,120)+' " + datajson["etime"].ToString() + "' and b.orderStatus=1) as 同步有效信息费,"
                      + "(select count(b.infoid) from tab_sync as b where " + company + " b.datatime between convert(varchar(10), a.datatime,120)+' " + datajson["stime"].ToString() + "' and convert(varchar(10), a.datatime,120)+' " + datajson["etime"].ToString() + "' and b.result=1 and b.orderStatus=1) as 回掉失败"
                      + " from spdataInfo_2016 as a " + param + " group by convert(varchar(10), a.datatime,120),datepart(day,a.datatime) order by datepart(day,a.datatime) desc";
            }
            else if (Convert.ToInt32(datajson["index"]) == 2)
            {
                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                    DateTime t = DateTime.Parse(datajson["edate"].ToString());
                    DateTime t1 = t.AddDays(1);
                    string eday = t1.ToShortDateString();
                    string[] temp = eday.Split('/');
                    if (temp[1].Length < 2)
                        temp[1] = "0" + temp[1];
                    if (temp[2].Length < 2)
                        temp[2] = "0" + temp[2];
                    datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];
                    param += "b.datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"].ToString() + "' and '" + datajson["edate"].ToString() + " " + datajson["etime"].ToString() + "' and ";

                }
                if (!string.IsNullOrEmpty(datajson["company"].ToString()))
                {
                    if (Convert.ToInt32(datajson["company"]) > 0)
                    {
                       
                            param += "b.companyID=" + datajson["company"].ToString()+" and ";
                    }
                }
                sql = "select a.name as 省份,"
                   + "(select count(b.infoid) from spdataInfo_2016 as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.ordertype=1) as 上行总量,"
                   + "(select count(b.infoid) from spdataInfo_2016 as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.ordertype=1 and b.resultCode=1) as 成功订购总量,"
                   + "COALESCE((select CASE WHEN sum(orderfee) IS NULL THEN 0 ELSE sum(orderfee) END from spdataInfo_2016 as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.ordertype=1 and b.orderStatus=1),0) as 有效信息费,"
                   + "COALESCE(Convert(decimal(18,2), CASE WHEN (select count(b.infoid) from spdataInfo_2016 as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.ordertype=1 and b.orderStatus=1)>0 THEN (COALESCE((select Convert(decimal(18,2),count(b.infoid)) from spdataInfo_2016 as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.ordertype=1 and b.orderStatus=1),0)/COALESCE((select Convert(decimal(18,2),count(b.infoid)) from spdataInfo_2016 as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.ordertype=1),0))*100 END),0) as 转化率,"
                   + "(select count(b.infoid) from tab_sync as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.ordertype=1 and b.orderStatus=1) as 同步成功总量,"
                   + "COALESCE(Convert(decimal(18,2),(CASE WHEN (select count(b.infoid) from tab_sync as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.ordertype=1 and b.orderStatus=1)>0 THEN (select  Convert(decimal(18,2),count(b.infoid)) from tab_sync as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.ordertype=1 and b.orderStatus=1)/(select Convert(decimal(18,2),count(b.infoid)) from spdataInfo_2016 as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name  and b.ordertype=1 and b.orderStatus=1) END)*100),0) as 同步率,"
                   + "COALESCE((select CASE WHEN sum(orderfee) IS NULL THEN 0 ELSE sum(orderfee) END from tab_sync as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.orderStatus=1 and b.ordertype=1),0) as 同步有效信息费,"
                   + "(select count(b.infoid) from tab_sync as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.result=1 and b.ordertype=1) as 回掉失败"
                   + " from areaInfo as a where a.parent_id=0  order by a.sort asc";
            }
            else
            {
                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                    DateTime t = DateTime.Parse(datajson["edate"].ToString());
                    DateTime t1 = t.AddDays(1);
                    string eday = t1.ToShortDateString();
                    string[] temp = eday.Split('/');
                    if (temp[1].Length < 2)
                        temp[1] = "0" + temp[1];
                    if (temp[2].Length < 2)
                        temp[2] = "0" + temp[2];
                    datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];

                    param += " and a.datatime between '" + datajson["sdate"].ToString() +"' and '" + datajson["edate"].ToString()+"'";
                    ptimes += "b.datatime between '" + datajson["sdate"].ToString() +"' and '" + datajson["edate"].ToString() +"' and ";
                }
                if (!string.IsNullOrEmpty(datajson["company"].ToString()))
                {
                    if (Convert.ToInt32(datajson["company"]) > 0)
                    {
                            param += " and a.companyID=" + datajson["company"].ToString();
                       
                    }
                }
                sql = "select c.name as 公司名称,a.orderFee as 资费,"
                    + "(select count(b.infoid) from spdataInfo_2016 as b where " + ptimes + " b.companyID=a.companyID and b.orderFee=a.orderFee and b.ordertype=1) as 上行总量,"
                    + "(select count(b.infoid) from spdataInfo_2016 as b where " + ptimes + " b.companyID=a.companyID and b.orderFee=a.orderFee and b.ordertype=1 and b.orderStatus=1) as 成功订购总量,"
                    + "(select CASE WHEN sum(orderfee) is null THEN 0 ELSE sum(orderfee) END from spdataInfo_2016 as b where " + ptimes + " b.companyID=a.companyID and b.orderFee=a.orderFee and b.ordertype=1 and b.orderStatus=1) as 有效信息费,"
                    + "Convert(decimal(18,2),CASE WHEN (select Convert(decimal(18,2),count(b.infoid)) from spdataInfo_2016 as b where  b.companyID=a.companyID and b.orderFee=a.orderFee and b.ordertype=1 and b.orderStatus=1)>0 THEN (select Convert(decimal(18,2),count(b.infoid)) from spdataInfo_2016 as b where " + ptimes + " b.companyID=a.companyID and b.orderFee=a.orderFee and b.ordertype=1 and b.orderStatus=1)/(select Convert(decimal(18,2),count(b.infoid)) from spdataInfo_2016 as b where " + ptimes + " b.companyID=a.companyID and b.orderFee=a.orderFee and b.ordertype=1)*100 ELSE 0 END) as 转化率,"
                    + "(select count(b.infoid) from tab_sync as b where " + ptimes + " b.companyID=a.companyID and b.orderFee=a.orderFee and b.ordertype=1 and b.orderStatus=1) as 同步成功总量,"
                    + "Convert(decimal(18,2),CASE WHEN (select Convert(decimal(18,2),count(b.infoid)) from tab_sync as b where b.companyID=a.companyID and b.orderFee=a.orderFee and b.orderStatus=1 and b.ordertype=1)>0 THEN (select Convert(decimal(18,2),count(b.infoid)) from tab_sync as b where " + ptimes + " b.companyID=a.companyID and b.orderFee=a.orderFee and b.orderStatus=1 and b.ordertype=1)/(select Convert(decimal(18,2),count(b.infoid)) from spdataInfo_2016 as b where " + ptimes + " b.companyID=a.companyID and b.orderFee=a.orderFee and b.ordertype=1 and b.orderStatus=1)*100 ELSE 0 END) as 同步率,"
                    + "(select CASE WHEN sum(orderfee) is null THEN 0 ELSE sum(orderfee) END from tab_sync as b where " + ptimes + " b.companyID=a.companyID and b.orderFee=a.orderFee and b.ordertype=1 and b.orderStatus=1) as 同步有效信息费,"
                    + "(select count(b.infoid) from tab_sync as b where " + ptimes + " b.companyID=a.companyID and b.orderFee=a.orderFee and b.ordertype=1 and b.result=1) as 回掉失败"
                    + " from spdataInfo_2016 as a left join company as c on c.infoid=a.companyID  where a.ordertype=1" + param + "group by c.name,a.orderFee,a.companyID ";
            }

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)

                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";

            return info;
        }


        public static string getStatisticsChannel(string search)
        {
            string info = null;
            string sql = null;
            string param = "";
            string times = "";
            string ptimes = "";
            string company = "";
            JObject datajson = JObject.Parse(search);
            string stime = datajson["stime"].ToString();
            string etime = datajson["etime"].ToString();

            if (stime.Length < 5)
                datajson["stime"] = "0" + stime;
            if (etime.Length < 5)
                datajson["etime"] = "0" + etime;


            if (Convert.ToInt32(datajson["index"]) == 1)
            {
                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                    DateTime t = DateTime.Parse(datajson["edate"].ToString());
                    DateTime t1 = t.AddDays(1);
                    string eday = t1.ToShortDateString();
                    string[] temp = eday.Split('/');
                    if (temp[1].Length < 2)
                        temp[1] = "0" + temp[1];
                    if (temp[2].Length < 2)
                        temp[2] = "0" + temp[2];
                    datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];
                    param += " where a.datatime between '" + datajson["sdate"].ToString() + "' and '" + datajson["edate"].ToString() + "'";
                }
                if (!string.IsNullOrEmpty(datajson["company"].ToString()))
                {
                    if (Convert.ToInt32(datajson["company"]) > 0)
                    {
                        company = "b.companyID=" + datajson["company"].ToString() + " and ";
                        if (param.Length > 0)
                            param += " and a.companyID=" + datajson["company"].ToString();
                        else
                            param += " where a.companyID=" + datajson["company"].ToString();
                    }
                }
                sql = "select convert(varchar(10), a.datatime,120) as 日期,"
                    + "(select count(b.infoid) from channel_2016 as b where " + company + " convert(varchar(10), b.datatime ,120)=convert(varchar(10), a.datatime,120)) as 上行总量,"
                    + "(select count(b.infoid) from channel_2016 as b where " + company + " convert(varchar(10), b.datatime ,120)=convert(varchar(10), a.datatime,120) and b.resultCode=0) as 成功订购总量,"
                    + "(select sum(fee) from channel_2016 as b where " + company + " convert(varchar(10), b.datatime ,120)=convert(varchar(10), a.datatime,120) and b.resultCode=0) as 有效信息费,"
                    + "Convert(decimal(18,2),(CASE WHEN (select count(b.infoid) from channel_2016 as b where " + company + " convert(varchar(10), b.datatime ,120)=convert(varchar(10), a.datatime,120) and b.resultCode=0)>0 THEN (select Convert(decimal(18,2),count(b.infoid)) from channel_2016 as b where " + company + " convert(varchar(10), b.datatime ,120)=convert(varchar(10), a.datatime,120) and b.resultCode=0)/(select Convert(decimal(18,2),count(b.infoid)) from channel_2016 as b where " + company + " convert(varchar(10), b.datatime ,120)=convert(varchar(10), a.datatime,120))*100 END)) as 转化率,"
                    + "(select count(b.infoid) from channel_notify as b where " + company + " b.resultCode=0 and REPLACE(CONVERT(varchar(10),a.datatime,120),'-','')=substring(b.StreamingNo,1,8)) as 同步成功总量,"
                    + "Convert(decimal(18,2),(CASE WHEN (select count(b.infoid) from channel_notify as b where " + company + " b.resultCode=0 and  REPLACE(CONVERT(varchar(10),a.datatime,120),'-','')=substring(b.StreamingNo,1,8))>0 THEN (select Convert(decimal(18,2),count(b.infoid)) from channel_notify as b where " + company + " b.resultCode=0 and REPLACE(CONVERT(varchar(10),a.datatime,120),'-','')=substring(b.StreamingNo,1,8))/(select Convert(decimal(18,2),count(b.infoid)) from channel_2016 as b where " + company + " convert(varchar(10), b.datatime ,120)=convert(varchar(10), a.datatime,120) and b.resultCode=0)*100 END)) as 同步率,"
                    + "(select sum(fee) from channel_notify as b where " + company + " REPLACE(CONVERT(varchar(10),a.datatime,120),'-','')=substring(b.StreamingNo,1,8) and b.resultCode=0) as 同步有效信息费,"
                    + "(select count(b.infoid) from channel_notify as b where " + company + " REPLACE(CONVERT(varchar(10),a.datatime,120),'-','')=substring(b.StreamingNo,1,8) and b.result=1) as 回掉失败"
                    + " from channel_2016 as a " + param + " group by convert(varchar(10), a.datatime,120) order by convert(varchar(10), a.datatime,120) desc";
            }
            else if (Convert.ToInt32(datajson["index"]) == 2)
            {
                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                    DateTime t = DateTime.Parse(datajson["edate"].ToString());
                    DateTime t1 = t.AddDays(1);
                    string eday = t1.ToShortDateString();
                    string[] temp = eday.Split('/');
                    if (temp[1].Length < 2)
                        temp[1] = "0" + temp[1];
                    if (temp[2].Length < 2)
                        temp[2] = "0" + temp[2];
                    datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];
                    param += "b.datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"].ToString() + "' and '" + datajson["edate"].ToString() + " " + datajson["etime"].ToString() + "' and ";
                    ptimes += "b.StreamingNo between '" + datajson["sdate"].ToString().Replace("-", "") + datajson["stime"].ToString().Replace(":", "") + "' and '" + datajson["edate"].ToString().Replace("-", "") + datajson["etime"].ToString().Replace(":", "") + "' and ";
                }
                if (!string.IsNullOrEmpty(datajson["company"].ToString()))
                {
                    if (Convert.ToInt32(datajson["company"]) > 0)
                    {

                        param += "b.companyID=" + datajson["company"].ToString() + " and ";
                        ptimes += "b.companyID=" + datajson["company"].ToString() + " and ";
                    }
                }
                sql = "select a.name as 省份,"
                    + "(select count(b.infoid) from channel_2016 as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name) as 上行总量,"
                    + "(select count(b.infoid) from channel_2016 as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.resultCode=0) as 成功订购总量,"
                    + "COALESCE((select sum(fee) from channel_2016 as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.resultCode=0),0) as 有效信息费,"
                    + "COALESCE(Convert(decimal(18,2), CASE WHEN (select count(b.infoid) from channel_2016 as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.resultCode=0)>0 THEN (COALESCE((select Convert(decimal(18,2),count(b.infoid)) from channel_2016 as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.resultCode=0),0)/COALESCE((select Convert(decimal(18,2),count(b.infoid)) from channel_2016 as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name),0))*100 END),0) as 转化率,"
                    + "(select count(b.infoid) from channel_notify as b where " + ptimes + " b.resultCode=0 and SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name) as 同步成功总量,"
                    + "COALESCE(Convert(decimal(18,2),(CASE WHEN (select count(b.infoid) from channel_notify as b where " + ptimes + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.resultCode=0)>0 THEN (select  Convert(decimal(18,2),count(b.infoid)) from channel_notify as b where " + ptimes + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.resultCode=0)/(select Convert(decimal(18,2),count(b.infoid)) from channel_2016 as b where " + param + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name AND b.resultCode=0) END)*100),0) as 同步率,"
                    + "COALESCE((select sum(fee) from channel_notify as b where " + ptimes + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.resultCode=0),0) as 同步有效信息费,"
                    + "(select count(b.infoid) from channel_notify as b where " + ptimes + " SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name and b.result=1) as 回掉失败"
                    +" from areaInfo as a where a.parent_id=0 order by a.sort asc";
            }
            else
            {
                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                    DateTime t = DateTime.Parse(datajson["edate"].ToString());
                    DateTime t1 = t.AddDays(1);
                    string eday = t1.ToShortDateString();
                    string[] temp = eday.Split('/');
                    if (temp[1].Length < 2)
                        temp[1] = "0" + temp[1];
                    if (temp[2].Length < 2)
                        temp[2] = "0" + temp[2];
                    datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];

                    param += "where a.datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"].ToString() + "' and '" + datajson["edate"].ToString() + " " + datajson["etime"].ToString() + "'";
                    times += "b.datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"].ToString() + "' and '" + datajson["edate"].ToString() + " " + datajson["etime"].ToString() + "' and ";
                    ptimes += "b.StreamingNo between '" + datajson["sdate"].ToString().Replace("-", "") + datajson["stime"].ToString().Replace(":", "") + "' and '" + datajson["edate"].ToString().Replace("-", "") + datajson["etime"].ToString().Replace(":", "") + "' and ";
                }
                if (!string.IsNullOrEmpty(datajson["company"].ToString()))
                {
                    if (Convert.ToInt32(datajson["company"]) > 0)
                    {

                        if (param.Length > 0)
                            param += " and a.companyID=" + datajson["company"].ToString();
                        else
                            param += " where a.companyID=" + datajson["company"].ToString();
                    }
                }
                sql = " select c.name as 公司名称,a.fee as 资费,"
                    + "(select count(b.infoid) from channel_2016 as b where " + times + " b.companyID=a.companyID) as 上行总量,"
                    + "(select count(b.infoid) from channel_2016 as b where " + times + " b.companyID=a.companyID and b.resultCode=0) as 成功订购总量,"
                    + "(select sum(fee) from channel_2016 as b where " + times + " b.companyID=a.companyID and b.resultCode=0) as 有效信息费,"
                    + "Convert(decimal(18,2),((select Convert(decimal(18,2),count(b.infoid)) from channel_2016 as b where " + times + " b.companyID=a.companyID and b.resultCode=0)/(select Convert(decimal(18,2),count(b.infoid)) from channel_2016 as b where " + times + " b.companyID=a.companyID))*100) as 转化率,"
                    + "(select count(b.infoid) from channel_notify as b where " + ptimes + " b.resultCode=0 and b.companyID=a.companyID) as 同步成功总量,"
                    + "Convert(decimal(18,2),(select Convert(decimal(18,2),count(b.infoid)) from channel_notify as b where " + ptimes + " b.resultCode=0 and b.companyID=a.companyID)/(select Convert(decimal(18,2),count(b.infoid)) from channel_2016 as b where " + times + " b.companyID=a.companyID and b.resultCode=0)) as 同步率,"
                    + "(select sum(fee) from channel_notify as b where " + ptimes + " b.companyID=a.companyID and b.resultCode=0) as 同步有效信息费,"
                    + "(select count(b.infoid) from channel_notify as b where " + ptimes + " b.companyID=a.companyID and b.result=1) as 回掉失败"
                    + " from channel_2016 as a left join company as c on c.infoid=a.companyID " + param + " group by c.name,a.fee,a.companyID";
            }

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)

                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";

            return info;
        }


        /// <summary>
        /// 点播查询统计
        /// </summary>
        /// <param name="search"></param>
        /// <returns></returns>
        public static string getStatisticsSF(string search)
        {
            string info = null;
            string sql = null;
            string param = "";
            JObject datajson = JObject.Parse(search);
            string stime = datajson["stime"].ToString();
            string etime = datajson["etime"].ToString();

            if (stime.Length < 5)
                datajson["stime"] = "0" + stime;
            if (etime.Length < 5)
                datajson["etime"] = "0" + etime;


            if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
            {
                
                string eday = datajson["edate"].ToString();
                string[] temp = eday.Split('-');
                if (temp[1].Length < 2)
                    temp[1] = "0" + temp[1];
                if (temp[2].Length < 2)
                    temp[2] = "0" + temp[2];
                datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];
                param += " and a.datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"] + "' and  DATEADD(MINUTE,+1,'" + datajson["edate"].ToString() + " " + datajson["etime"] + "')";

                
            }
            if (!string.IsNullOrEmpty(datajson["company"].ToString()))
            {
                if (Convert.ToInt32(datajson["company"]) > 0)
                    param += " and a.companyID=" + datajson["company"].ToString();
            }

            if (Convert.ToInt32(datajson["index"]) == 1)
            {
                sql = "select CONVERT(varchar(10),a.datatime,120) AS 日期,"
                     + "count(a.infoid) as 上行总量,"
                     + "COUNT(DISTINCT(a.mobile)) as 独立用户,"
                     + "sum(case when a.resultCode=0 then 1 else 0 end) as 成功订购总量,"
                     //+ "0 as 超额扣费,"
                     + "sum(case when a.resultCode=0 then a.fee else 0 end) as 有效信息费,"
                     + "case when sum(case when a.resultCode=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when a.resultCode=0 then 1 else 0 end))/Convert(decimal(18,2),count(a.infoid))*100) else 0 end as 转化率,"
                     + "sum(case when b.resultCode=0 then 1 else 0 end) as 同步成功总量,"
                     + "case when sum(case when b.resultCode=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when b.resultCode=0 then 1 else 0 end))/ Convert(decimal(18,2),sum(case when a.resultCode=0 then 1 else 0 end))*100) else 0 end as 同步率,"
                     + "sum(case when b.resultCode=0 then b.fee else 0 end) as 同步有效信息费,"
                     + "sum(case when b.result=1 then 1 else 0 end) as 回掉失败"
                     + " from tab_2016 as a WITH(NOLOCK) left join tab_notify as b on a.infoid=b.infoid where a.buyid=1" + param
                     + " group by convert(varchar(10), a.datatime,120) order by convert(varchar(10), a.datatime,120) desc";
            }
            else if (Convert.ToInt32(datajson["index"]) == 2)
            {
               
                sql = "select a.name as 省份,"
                    + "sum(case when b.buyID=1" + param.Replace("a.","b.") + " then 1 else 0 end) as 上行总量,"
                    + "count(DISTINCT(case when b.buyID=1" + param.Replace("a.", "b.") + " then b.mobile end)) as 独立用户,"
                    + "sum(case when b.buyID=1" + param.Replace("a.", "b.") + " and b.resultCode=0 then 1 else 0 end) as 成功订购总量,"
                    //+ "0 as 超额扣费,"
                    + "sum(case when b.buyID=1" + param.Replace("a.", "b.") + " and b.resultCode=0 then b.fee else 0 end) as 有效信息费,"
                    + "case when sum(case when b.buyID=1" + param.Replace("a.", "b.") + " and b.resultCode=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when b.buyID=1" + param.Replace("a.", "b.") + " and b.resultCode=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when b.buyID=1" + param.Replace("a.", "b.") + " then 1 else 0 end))*100) else 0 end as 转化率,"
                    + "sum(case when c.buyID=1" + param.Replace("a.", "c.") + " and c.resultCode=0 then 1 else 0 end) 同步成功总量,"
                    + "case when sum(case when c.buyID=1" + param.Replace("a.", "c.") + " and c.resultCode=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.buyID=1" + param.Replace("a.", "c.") + " and c.resultCode=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when b.buyID=1" + param.Replace("a.", "b.") + " and b.resultCode=0 then 1 else 0 end))*100) else 0 end as  同步率,"
                    + "sum(case when c.buyID=1" + param.Replace("a.", "c.") + " and c.resultCode=0 then c.fee else 0 end) as 同步有效信息费,"
                    + "sum(case when c.buyID=1" + param.Replace("a.", "c.") + " and c.result=1 then 1 else 0 end) as 回掉失败"
                    + " from areaInfo as a left join tab_2016 as b WITH(NOLOCK) on SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name left join tab_notify as c on c.infoid=b.infoid where a.parent_id=0 group by a.name,a.sort order by a.sort";
            }
            else
            {
               sql="select a.name as 公司名称,"
                +"case when b.fee is null then 0 else b.fee end as 资费,"
                +"count(b.infoid) as 上行总量,"
                +"COUNT(DISTINCT(b.mobile)) as 独立用户,"
                +"sum(case when b.resultCode=0 then 1 else 0 end) as 成功订购总量,"
                //+ "0 as 超额扣费,"
                +"sum(case when b.resultCode=0 then b.fee else 0 end) as 有效信息费,"
                +"case when sum(case when b.resultCode=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when b.resultCode=0 then 1 else 0 end))/Convert(decimal(18,2),count(b.infoid))*100) else 0 end as 转化率,"
                +"sum(case when c.resultCode=0 then 1 else 0 end) as 同步成功总量,"
                +"case when sum(case when c.resultCode=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.resultCode=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when b.resultCode=0 then 1 else 0 end))*100) else 0 end as 同步率,"
                +"sum(case when c.resultCode=0 then c.fee else 0 end) as 同步有效信息费,"
                +"sum(case when c.result=1 then 1 else 0 end) as 回掉失败"
                + " from company as a left join tab_2016 as b WITH(NOLOCK) on b.companyID=a.infoid left join tab_notify as c on c.infoid=b.infoid"
                + " where b.buyid=1" + param.Replace("a.","b.") + " group by a.name,b.fee order by a.name";
            }

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";
           

            return info;
        }

       
       
        /// <summary>
        /// 获取超额扣费数据
        /// </summary>
        /// <param name="search"></param>
        /// <returns></returns>
        public static string getExcessFeeData(string search)
        {
            string info = null;
            string param = "";
            JObject datajson = JObject.Parse(search);
            /*string stime = datajson["stime"].ToString();
            string etime = datajson["etime"].ToString();

            if (stime.Length < 5)
                datajson["stime"] = "0" + stime;
            if (etime.Length < 5)
                datajson["etime"] = "0" + etime;
            */

            if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
            {

                string eday = datajson["edate"].ToString();
                string[] temp = eday.Split('-');
                if (temp[1].Length < 2)
                    temp[1] = "0" + temp[1];
                if (temp[2].Length < 2)
                    temp[2] = "0" + temp[2];
                datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];
                param += " and a.datatime between '" + datajson["sdate"].ToString() + "' and  DATEADD(day,+1,'" + datajson["edate"].ToString() + "')";


            }
            if (!string.IsNullOrEmpty(datajson["company"].ToString()))
            {
                if (Convert.ToInt32(datajson["company"]) > 0)
                    param += " and a.companyID=" + datajson["company"].ToString();
            }

            string dic = getDictionary("信元点播金额");
            if (!string.IsNullOrEmpty(dic))
            {
                JObject value = JObject.Parse(dic);
                string sql = "select b.name,mobile,sum(a.fee) as fee,convert(varchar(10), a.datatime,120) as date from tab_2016  as a WITH(NOLOCK) left join company as b on b.infoid=a.companyID where a.buyid=1 and a.resultCode=0 " + param + " group by b.name,a.mobile,convert(varchar(10), a.datatime,120) having sum(fee)>" + value["value"].ToString();
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

                if (dt.Rows.Count > 0)
                    info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";
            }
            return info;
        }

        /// <summary>
        /// 获取地区列表
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public static string getAreaInfo(int id)
        {
            string sql = "SELECT b.*,(select count(a.id) from areaInfo a where ltrim(rtrim(b.id))=ltrim(rtrim(a.parent_id))) childnode from areaInfo b WHERE b.parent_id=" + id + " ORDER BY b.id";
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            IList<treeModel> model = new List<treeModel>();
            treeModel def = new treeModel();
            def.infoid = -1;
            def.value = "-1";
            def.name = "全国";
            def.childnode = 1;
            model.Add(def);
            foreach (DataRow row in dt.Rows)
            {
                treeModel item = new treeModel();
                item.infoid = Convert.ToInt32(row["id"]);
                item.value = row["name"].ToString();
                item.name = row["name"].ToString();
                item.childnode = Utils.StrToInt(row["childnode"], 0);
                model.Add(item);
            }

            return JsonConvert.SerializeObject(model); 
        }

        public static string getAreaInfo()
        {
            string sql = " SELECT name ,id as infoid from areaInfo where parent_id=0 and name not in('[未识别]','[无号码]') group by name,id";
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            IList<treeModel> model = new List<treeModel>();
            treeModel def = new treeModel();
            def.infoid = -1;
            def.value = "-1";
            def.name = "全国";
            def.childnode = 1;
            model.Add(def);
            foreach (DataRow row in dt.Rows)
            {
                treeModel item = new treeModel();
                item.infoid = Convert.ToInt32(row["infoid"]);
                item.value = row["name"].ToString();
                item.name = row["name"].ToString();
               // item.childnode = Utils.StrToInt(row["childnode"], 0);
                model.Add(item);
            }

            return JsonConvert.SerializeObject(model);
        }

        /// <summary>
        /// 获取下游公司列表
        /// </summary>
        /// <returns></returns>
        public static string getCompanyList()
        {
            string info = "{data:[]}";
            string sql = "select * from company order by infoid";
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {
                DataRow dr = dt.NewRow();
                dr["infoid"] = -1;
                dr["name"]="全部公司";
                dt.Rows.Add(dr);
                dt.AcceptChanges();

                DataTable dcopy=dt.Copy();
                DataView dv = dt.DefaultView;
                dv.Sort = "name asc";
                dcopy = dv.ToTable();

                info = "{data:" + JsonConvert.SerializeObject(dcopy, new DataTableConverter()) + "}";
            }

            return info;
        }

        public static string getCompanyList(int conduitid)
        {
            string info = "{data:[]}";
            string sql = "select * from company order by infoid";
            if (conduitid > 0)
                sql = "select b.infoid,b.name from action as a left join company as b on b.infoid=a.companyID where a.conduitid=" + conduitid + " group by b.infoid,b.name order by b.name";
           
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {
               

                DataTable dcopy = dt.Copy();
                DataView dv = dt.DefaultView;
                dv.Sort = "name asc";
                dcopy = dv.ToTable();

                DataRow dr = dcopy.NewRow();
                dr["infoid"] = -1;
                dr["name"] = "全部公司";
                dcopy.Rows.Add(dr);
                dcopy.AcceptChanges();


                info = "{data:" + JsonConvert.SerializeObject(dcopy, new DataTableConverter()) + "}";
            }

            return info;
        }


        public static string getCompanyList(int conduitid,int productid)
        {
            string info = "{data:[]}";
            string sql = "select * from company order by infoid";
            if (conduitid > 0)
                sql = "select b.infoid,b.name from action as a left join company as b on b.infoid=a.companyID where a.conduitid=" + conduitid + " and a.productid=" + productid + " order by b.name";

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {
               
                DataTable dcopy = dt.Copy();
                DataView dv = dt.DefaultView;
                dv.Sort = "name asc";
                dcopy = dv.ToTable();


                DataRow dr = dcopy.NewRow();
                dr["infoid"] = -1;
                dr["name"] = "全部公司";
                dcopy.Rows.Add(dr);
                dcopy.AcceptChanges();

                info = "{data:" + JsonConvert.SerializeObject(dcopy, new DataTableConverter()) + "}";
            }

            return info;
        }


        /// <summary>
        /// 获取下游公司列表
        /// </summary>
        /// <param name="pagesize"></param>
        /// <param name="currentpage"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static string getCompanyList(int pagesize, int currentpage, string param)
        {
           return getCompanyList(pagesize, currentpage, param, 0);
        }


        /// <summary>
        /// 获取下游公司列表
        /// </summary>
        /// <param name="pagesize"></param>
        /// <param name="currentpage"></param>
        /// <param name="param"></param>
        /// <param name="flag"></param>
        /// <returns></returns>
        public static string getCompanyList(int pagesize, int currentpage, string param, int flag)
        {
            string info = "{data:[]}";
            string sql = null;
            string paramstr = "";
            if (!string.IsNullOrEmpty(param))
            {
                if (flag == 0)
                    paramstr = " and a.name like '%" + param + "%'";
                else if (flag == 1)
                    paramstr = " and b.invoice like '%" + param + "%'";
                else if (flag == 2)
                    paramstr = " and b.contact like '%" + param + "%'";
            }
            int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "SELECT COUNT(a.infoid) FROM company as a WITH(NOLOCK) left join cppropertyInfo as b on b.companyid=a.infoid WHERE a.infoid>0" + paramstr));

            if (currentpage == 1)
                sql = "select top " + pagesize + " * from company as a WITH(NOLOCK) left join cppropertyInfo as b on b.companyid=a.infoid where a.infoid>0 " + paramstr + " order by a.infoid desc";
            else
                sql = "SELECT TOP " + pagesize + " * from company as a WITH(NOLOCK) left join cppropertyInfo as b on b.companyid=a.infoid WHERE a.infoid <=(SELECT MIN(infoid)"
                   + " FROM (SELECT TOP " + ((currentpage - 1) * pagesize + 1) + " [infoid] FROM company WITH(NOLOCK)"
                   + " WHERE infoid>0 " + paramstr + " ORDER BY [infoid] DESC) AS [tblTmp]) and a.infoid>0 " + paramstr + " ORDER BY a.infoid DESC";

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",total:" + TotalRecords + "}";

            return info;
        }


        /// <summary>
        /// 获取下游明细
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static string getCompanyInfo(int infoid)
        {
            string info=null;
            string sql = "select * from company where infoid="+infoid;
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            if (dt.Rows.Count > 0)
                info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[","").Replace("]","");

            return info;
        }


        public static DataTable getCompanyInfo(string name)
        {
            string sql = "select * from company where name like '%"+name+"%'";
            return SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
        }

        //电信点播包月时时数据
        public static string getInfo(int pagesize, int currentpage, string param)
        {
            string info = "{data:[],total:0}";
            string sql = null;
            string paramstr = "";
      
            if (!string.IsNullOrEmpty(param))
            {
                JObject datajson = JObject.Parse(param);
                string[] mobile = datajson["mobile"].ToString().Replace(" ", ",").Split(',');
                for (int i = 0; i < mobile.Length; i++)
                {
                    if (!string.IsNullOrEmpty(mobile[i].Trim()))
                        paramstr += ",'" + mobile[i].Trim() + "'";
                } 
                if (!string.IsNullOrEmpty(paramstr))
                        paramstr = " and mobile in(" + paramstr.Substring(1) + ") ";

                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                        DateTime t = DateTime.Parse(datajson["edate"].ToString());
                        DateTime t1 = t.AddDays(1);
                        string eday = t1.ToShortDateString();
                        string sday = DateTime.Parse(datajson["sdate"].ToString()).ToShortDateString();
                     
                        paramstr += " and a.datatime between '" + sday + "' and '" + eday + "'";
                       
                 }
              
            }
            int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "SELECT COUNT(a.infoid) FROM Tab_2016 as a WITH(NOLOCK) WHERE a.infoid>59 and (status is null or status like '成功%')" + paramstr));
            if (currentpage == 1)
                sql = "SELECT TOP " + pagesize + " a.infoid,b.name,a.companyid,a.mobile,a.orderresult,a.fee,a.area,"
                    + "a.buyname as 业务类型,c.name as 产品名称,"
                    +"(select CASE WHEN count(infoid)>0 THEN 1 END from refund where tablename='tab_2016' and dataid=a.infoid) refund,"
                    + "(select CASE WHEN total>0 THEN total END from refund where tablename='tab_2016' and dataid=a.infoid) total,"
                    + "(select CASE WHEN info is not null THEN info END from refund where tablename='tab_2016' and dataid=a.infoid) info,"
                    + "(select CASE WHEN note is not null or note<>'' THEN note END from refund where tablename='tab_2016' and dataid=a.infoid) note,"
                    + "(select CASE WHEN date is not null THEN  CONVERT(varchar(20),date , 20) END from refund where tablename='tab_2016' and dataid=a.infoid) date,"
                    + "(select TOP 1 b.typeID from blacklist as b where b.mobile=a.mobile) black,"
                    + "(select CASE WHEN count(b.infoid)=0 THEN 0 END from tab_notify as b where b.infoid=a.infoid) notify,"
                    + "a.status,CONVERT(varchar(21), a.datatime, 20) as datatime FROM Tab_2016 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyid"
                    +" right join product as c on c.infoid=a.productID"
                    + " where a.infoid>59 and (status is null or status like '成功%')" + paramstr + " ORDER BY a.infoid DESC";
            else
            {
                sql = "SELECT TOP " + pagesize + " a.infoid,b.name,a.companyid,a.mobile,a.orderresult,a.fee,a.area,a.status,"
                    + "a.buyname as 业务类型,c.name as 产品名称,"
                    +"(select CASE WHEN count(infoid)>0 THEN 1 END from refund where tablename='tab_2016' and dataid=a.infoid) refund,"
                    + "(select CASE WHEN total>0 THEN total END from refund where tablename='tab_2016' and dataid=a.infoid) total,"
                    + "(select CASE WHEN  info is not null THEN info END from refund where tablename='tab_2016' and dataid=a.infoid) info,"
                    + "(select CASE WHEN note is not null or note<>'' THEN note END from refund where tablename='tab_2016' and dataid=a.infoid) note,"
                    + "(select CASE WHEN date is not null THEN CONVERT(varchar(20),date , 20) END from refund where tablename='tab_2016' and dataid=a.infoid) date,"
                    + "(select TOP 1 b.typeID from blacklist as b where b.mobile=a.mobile) black,"
                    + "(select CASE WHEN count(b.infoid)=0 THEN 0 END from tab_notify as b where b.infoid=a.infoid) notify,"
                    + "CONVERT(varchar(21), a.datatime, 20) as datatime FROM [Tab_2016] as a WITH(NOLOCK) left join company as b on b.infoid=a.companyid"
                    +" right join product as c on c.infoid=a.productID WHERE a.infoid <=(SELECT MIN(infoid)"
                    +" FROM (SELECT TOP " + ((currentpage - 1) * pagesize + 1) + " [infoid] FROM Tab_2016 WITH(NOLOCK)"
                    + " WHERE infoid>59 and (status is null or status like '成功%')" + paramstr + " ORDER BY [infoid] DESC) AS [tblTmp]) and a.infoid>59 and"
                    +" (status is null or status like '成功%')" + paramstr + " ORDER BY a.infoid DESC";
            }
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",total:" + TotalRecords + "}";


            return info;
        }


        /// <summary>
        /// 支付平台时时数据
        /// </summary>
        /// <param name="pagesize"></param>
        /// <param name="currentpage"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static string getPayInfo(int pagesize, int currentpage, string param)
        {
            string info = "data:[]";
            string sql = null;
            string paramstr = "";
            if (!string.IsNullOrEmpty(param))
            {
                JObject datajson = JObject.Parse(param);
                string[] mobile=datajson["mobile"].ToString().Replace(" ",",").Split(',');
                for (int i = 0; i < mobile.Length; i++)
                {
                    if(!string.IsNullOrEmpty(mobile[i].Trim()))
                      paramstr += " or a.mobile like '" + mobile[i].Trim() + "%'";
                }

                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                    DateTime t = DateTime.Parse(datajson["edate"].ToString());
                    DateTime t1 = t.AddDays(1);
                    string eday = t1.ToShortDateString();
                    string sday = DateTime.Parse(datajson["sdate"].ToString()).ToShortDateString();
                    param += " where a.datatime between '" + sday + "' and '" + eday + "'";
                }
                if (!string.IsNullOrEmpty(paramstr))
                      paramstr+= " and a.datatime between " + param;

            }
              
            int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "SELECT COUNT(a.infoid) FROM spdataInfo_2016 as a WHERE  " + paramstr));


            if (currentpage == 1)
                sql = "SELECT TOP " + pagesize + " a.infoid,b.name,a.OrderNo,a.result_msg,a.phoneNo,a.Area,a.orderFee,a.productNo,a.resultCode ,a.orderStatus,a.datatime,a.companyID,a.ordertype FROM spdataInfo_2016 as a left join company as b on b.infoid=a.companyID where  " + paramstr + " ORDER BY a.infoid DESC";
            else
            {
                sql = "SELECT TOP " + pagesize + " a.infoid,b.name,a.OrderNo,a.result_msg,a.phoneNo,a.Area,a.orderFee,a.productNo,a.resultCode ,a.orderStatus,a.datatime,a.companyID,a.ordertype  FROM spdataInfo_2016 as a left join company as b on b.infoid=a.companyID WHERE a.infoid <=(SELECT MIN(infoid) ";
                sql += "FROM (SELECT TOP " + ((currentpage - 1) * pagesize+1) + " infoid FROM spdataInfo_2016  WHERE a.companyID=4" + paramstr.Replace("a.","") + " ORDER BY a.infoid DESC) AS [tblTmp]) " + paramstr + " ORDER BY a.infoid DESC";
            }

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)

                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",total:" + TotalRecords + "}";

            return info;
        }


        /// <summary>
        /// 设置退费
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string setRefund(string data,string total)
        {

            string flag = "设置完成！";
            string param = null;
           
            SqlConnection conn = new SqlConnection(SqlHelper.ConnectionStringLocalTransaction);
            conn.Open();
            using (SqlTransaction trans = conn.BeginTransaction())
            {
                try
                {  
                   JObject value = JObject.Parse(total);

                   string dsql = "delete refund where dataid in(" + data + ")";
                   SqlHelper.ExecuteNonQuery(trans, CommandType.Text, dsql);

                   string sql = "INSERT INTO [refund]([area],[tablename],[companyID],[mobile],[price],[dataid],[notify],[datatime],[info],[note],[total])"
                              + " SELECT area,'tab_2016',a.companyid,a.mobile,a.fee,a.infoid,"
                              + "(select CASE WHEN count(b.infoid)=0 THEN '未同步' END from tab_notify as b where b.infoid=a.infoid) notify,"
                              + "CONVERT(varchar(21), a.datatime, 20) as datatime,'" + value["info"].ToString() + "','" + value["note"].ToString() + "'," + value["total"].ToString()
                              + " FROM Tab_2016 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyid where a.infoid in(" + data + ")  ORDER BY a.infoid DESC";


                    SqlHelper.ExecuteNonQuery(trans, CommandType.Text, sql);

                    /*JArray value = JArray.Parse(total);
                    JObject raw = JObject.Parse(data);
                    for (int i = 0; i < value.Count; ++i)  //遍历JArray
                    {
                        JObject o = JObject.Parse(value[i].ToString());

                        string dsql = "delete refund where dataid=" + o["infoid"].ToString();
                        SqlHelper.ExecuteNonQuery(trans, CommandType.Text, dsql);

                        
                        param = "'" + o["area"].ToString() + "','tab_2016'," + o["companyid"].ToString() + ",'" + o["mobile"].ToString() + "'," + o["fee"].ToString() + "," + o["infoid"].ToString() + ",'" + o["notify"].ToString() + "','" + o["datatime"].ToString() + "','" + raw["info"].ToString() + "'," + raw["total"].ToString();
                        string sql = "INSERT INTO [refund]([area],[tablename],[companyID],[mobile],[price],[dataid],[notify],[datatime],[info],[total]) values(" + param + ")";

                        SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);

                    }*/

                    trans.Commit();
                  
                }
                catch (Exception e)
                {
                    flag = "设置失败！";
                    trans.Rollback();
                    LogHelper.WriteLog(typeof(dataManage),"============>setRefund异常[" + e.ToString() + "]");

                }
            }
            conn.Close();


            return flag;
            
        }

        /// <summary>
        /// 设置黑名单
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string setShield(string data)
        {

            string flag = "设置完成！";
          

            SqlConnection conn = new SqlConnection(SqlHelper.ConnectionStringLocalTransaction);
            conn.Open();
            using (SqlTransaction trans = conn.BeginTransaction())
            {
                try
                {
                    string[] mobile=data.Split(',');

                    string temp = string.Empty;
                    for (int i = 0; i < mobile.Length; i++)
                    {
                        temp += ",'" + mobile[i] + "'";
                    }

                    string sql1 = "delete blacklist where mobile in(" + temp.Substring(1) + ")";
                    SqlHelper.ExecuteNonQuery(trans, CommandType.Text, sql1);

                    for(int i=0;i<mobile.Length;i++)
                    {
                        string sql = "INSERT INTO [blacklist]([mobile]) VALUES('" + mobile[i] + "')";
                        SqlHelper.ExecuteNonQuery(trans, CommandType.Text, sql);

                    }

                    trans.Commit();

                }
                catch (Exception e)
                {
                    flag = "设置失败！";
                    trans.Rollback();
                    LogHelper.WriteLog(typeof(dataManage), "============>setShiel异常[" + e.ToString() + "]");
                }
            }
            conn.Close();
            return flag;
        }



        /// <summary>
        /// 退费并屏蔽
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string setUnion(string data, string total)
        {

            string flag = "设置完成！";
           

            SqlConnection conn = new SqlConnection(SqlHelper.ConnectionStringLocalTransaction);
            conn.Open();
            using (SqlTransaction trans = conn.BeginTransaction())
            {
                try
                {  
                    JObject value = JObject.Parse(total);

                    string sql1 = "INSERT INTO [refund]([area],[tablename],[companyID],[mobile],[price],[dataid],[notify],[datatime],[info],[total])"
                             + " SELECT a.arae,'tab_2016',a.companyid,a.mobile,a.fee,a.infoid,"
                             + "(select CASE WHEN count(b.infoid)=0 THEN '未同步' END from tab_notify as b where b.infoid=a.infoid) notify,"
                             + "CONVERT(varchar(21), a.datatime, 20) as datatime,'" + value["info"].ToString() + "','" + value["total"].ToString() + "'"
                             + " FROM Tab_2016 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyid where a.infoid in(" + data + ")  ORDER BY a.infoid DESC";

                    SqlHelper.ExecuteNonQuery(trans, CommandType.Text, sql1);


                    string sql2 = "INSERT INTO [blacklist]([mobile],[rulesid]) select mobile,3 FROM Tab_2016 infoid in(" + data + ")";
                    SqlHelper.ExecuteNonQuery(trans, CommandType.Text, sql2);

                    trans.Commit();
                }
                catch (Exception e)
                {
                    flag = "设置失败！";
                    trans.Rollback();
                    LogHelper.WriteLog(typeof(dataManage), "============>setUnion异常[" + e.ToString() + "]");
                }
            }
            conn.Close();
            return flag;
        }



        /// <summary>
        /// 获取字典定义数据
        /// </summary>
        /// <returns></returns>
        public static string getDictionary(int id)
        {
            string info = "{data:[]}";
            string sql = "select value,name from dictionary where pid="+id+" order by infoid";
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {

                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";
            }

            return info;
        }

        public static string getDictionary(string id)
        {
            string info = "{data:}";
            string sql = "select value,name from dictionary where name='" + id + "'";
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {

                info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[","").Replace("]","");
            }

            return info;
        }

        public static string getDictionaryItem(string id)
        {
            string json = string.Empty;
            string sql = "select b.value,b.name from dictionary as a left join dictionary as b on b.pid=a.infoid where a.name='" + id + "'";
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            foreach (DataRow dr in dt.Rows)
                json += string.Format(",'{0}':'{1}'", dr["name"].ToString(), dr["value"].ToString());
            
            return "{"+json.Substring(1)+"}";
        }
       

        public static string getDictList(string id)
        {
            string info = "{data:}";
            string sql = "select value,name from dictionary where name='" + id + "'";
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {

                info = "{data:"+JsonConvert.SerializeObject(dt, new DataTableConverter())+"}";
            }

            return info;
        }


        /// <summary>
        /// 获取退费数据
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        public static string getRefund(string data, int currentpage, int pagesize)
        {
            string info = "{data:[]}";
            string param="";
            
            string sql=null;
            if (!string.IsNullOrEmpty(data))
            {
                JObject datajson = JObject.Parse(data);
                string[] mobile = datajson["mobile"].ToString().Replace(" ", ",").Split(',');
                for (int i = 0; i < mobile.Length; i++)
                {
                    if (!string.IsNullOrEmpty(mobile[i].Trim()))
                        param += ",'" + mobile[i].Trim() + "'";
                }
                if (!string.IsNullOrEmpty(param))
                    param = " and a.mobile in(" + param.Substring(1) + ")";

                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                    DateTime t = DateTime.Parse(datajson["edate"].ToString());
                    DateTime t1 = t.AddDays(1);
                    string eday = t1.ToShortDateString();
                    string sday = DateTime.Parse(datajson["sdate"].ToString()).ToShortDateString();

                    if (datajson["rbauto"].ToString() == "0")
                        param += " and a.datatime between '" + sday + "' and '" + eday + "'";
                    else
                        param += " and a.date between '" + sday + "' and '" + eday + "'";
                  
                }

                if (!string.IsNullOrEmpty(datajson["company"].ToString()))
                {
                    if (Convert.ToInt32(datajson["company"]) > 0)
                        param += " and a.companyID=" + datajson["company"].ToString();
                }
            }

            int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "SELECT COUNT(a.infoid) FROM refund as a where a.infoid>0" + param));

            if (currentpage == 1)

                sql = "SELECT top " + pagesize + " b.name as 公司,a.area as 省份,a.mobile as 卡号,a.price as 资费,a.total as 退费金额, case when (a.notify<>'未同步' or a.notify is null) then '' else a.notify end as 通知,"
                   +"(case when (select sum(price) from refund as c where c.mobile=a.mobile and c.notify='未同步' and c.companyID=a.companyID)>0 then (select sum(price ) from refund as c"
                   +" where c.mobile=a.mobile and c.companyID=a.companyID)-(select sum(price) from refund as c where c.mobile=a.mobile and c.notify='未同步' and c.companyID=a.companyID)"
                   + " else a.total end) as 下游退费,note as 备注,convert(varchar(10), a.datatime,120) as 订购日期,convert(varchar(10), a.date,120) as 退费日期"
                   + " FROM [dataInfo].[dbo].[refund] as a left join company as b on b.infoid=a.companyID where a.infoid>0" + param+" ORDER BY a.infoid DESC";
            else
            {
                sql = "SELECT top " + pagesize + " b.name as 公司,a.area as 省份,a.mobile as 卡号,a.price as 资费,a.total as 退费金额, case when (a.notify<>'未同步' or a.notify is null) then '' else a.notify end as 通知,"
                 + "(case when (select sum(price) from refund as c where c.mobile=a.mobile and c.notify='未同步' and c.companyID=a.companyID)>0 then (select sum(price ) from refund as c"
                 + " where c.mobile=a.mobile and c.companyID=a.companyID)-(select sum(price) from refund as c where c.mobile=a.mobile and c.notify='未同步' and c.companyID=a.companyID)"
                 + " else a.total end) as 下游退费,note as 备注,convert(varchar(10), a.datatime,120) as 订购日期,convert(varchar(10), a.date,120) as 退费日期"
                 + " FROM [refund] as a left join company as b on b.infoid=a.companyID WHERE a.infoid <=(SELECT MIN(infoid) FROM (SELECT TOP " + ((currentpage - 1) * pagesize + 1) + " infoid FROM refund where infoid>0" + param.Replace("a.", "") + " ORDER BY infoid DESC) AS [tblTmp])" + param + " ORDER BY a.infoid DESC";
            }

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",total:" + TotalRecords + "}";

            return info;
        }


        /// <summary>
        /// 将DataTable中数据写入到CSV文件中
        /// </summary>
        /// <param name="dt">提供保存数据的DataTable</param>
        /// <param name="fileName">CSV的文件路径</param>
        public static bool exportRefund(DataTable dt, string fileName)
        {
            bool flag = true;
            FileStream fs = new FileStream(fileName, System.IO.FileMode.Create, System.IO.FileAccess.Write);
            StreamWriter sw = new StreamWriter(fs, System.Text.Encoding.Default);
            string data = "";
            try
            {
                //写出列名称
                for (int i = 0; i < dt.Columns.Count; i++)
                {
                    data += dt.Columns[i].ColumnName.ToString();
                    if (i < dt.Columns.Count - 1)
                    {
                        data += ",";
                    }
                }
                sw.WriteLine(data);

                //写出各行数据
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    data = "";
                    for (int j = 0; j < dt.Columns.Count; j++)
                    {
                        data += dt.Rows[i][j].ToString();
                        if (j < dt.Columns.Count - 1)
                        {
                            data += ",";
                        }
                    }
                    sw.WriteLine(data);
                }
            }
            catch (Exception e)
            {
                sw.Close();
                fs.Close();
                LogHelper.WriteLog(typeof(dataManage), "============>exportRefund异常[" + e.ToString() + "]");
                return false;
            }
            sw.Close();
            fs.Close();

            return flag;
        }


        /// <summary>
        /// 导出数据
        /// </summary>
        /// <returns></returns>
        public static string exportData(string data)
        {
             string info = null;
             string names=DateTime.Now.ToString("yyyyMMddhhmmss")+".csv";
             string path = AppDomain.CurrentDomain.SetupInformation.ApplicationBase + ConfigurationManager.AppSettings["cvspath"];

             string param = "";

             string sql = null;
             if (!string.IsNullOrEmpty(data))
             {
                 JObject datajson = JObject.Parse(data);
                 string[] mobile = datajson["mobile"].ToString().Replace(" ", ",").Split(',');
                 for (int i = 0; i < mobile.Length; i++)
                 {
                     if (!string.IsNullOrEmpty(mobile[i].Trim()))
                         param += ",'" + mobile[i].Trim() + "'";
                 }
                 if (!string.IsNullOrEmpty(param))
                     param = " and mobile in(" + param.Substring(1) + ")";

                 if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                 {
                     DateTime t = DateTime.Parse(datajson["edate"].ToString());
                     DateTime t1 = t.AddDays(1);
                     string eday = t1.ToShortDateString();
                     string sday = DateTime.Parse(datajson["sdate"].ToString()).ToShortDateString();

                     if (datajson["rbauto"].ToString()=="0")
                       param += " and a.datatime between '" + sday + "' and '" + eday + "'";
                     else
                       param += " and a.date between '" + sday + "' and '" + eday + "'";

                 }

                 if (!string.IsNullOrEmpty(datajson["company"].ToString()))
                 {
                     if (Convert.ToInt32(datajson["company"]) > 0)
                         param += " and a.companyID=" + datajson["company"].ToString();
                 }
             }

              sql = "SELECT b.name as 公司,a.area as 省份,a.mobile as 卡号,a.price as 资费,a.total as 退费金额, case when (a.notify<>'未同步' or a.notify is null) then '' else a.notify end as 通知,"
                  + "(case when (select sum(price) from refund as c where c.mobile=a.mobile and c.notify='未同步' and c.companyID=a.companyID)>0 then (select sum(price ) from refund as c"
                  + " where c.mobile=a.mobile and c.companyID=a.companyID)-(select sum(price) from refund as c where c.mobile=a.mobile and c.notify='未同步' and c.companyID=a.companyID)"
                  + " else a.total end) as 下游退费,convert(varchar(10), a.datatime,120) as 订购日期,convert(varchar(10), a.date,120) as 退费日期"
                  + " FROM [dataInfo].[dbo].[refund] as a left join company as b on b.infoid=a.companyID where a.infoid>0" + param + " ORDER BY a.infoid DESC";

             DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {
                bool flag=exportRefund(dt, path+names);
                if (flag)
                    info =names;
            }

            return info;
        }


        /// <summary>
        /// 导出统计数据
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static string exportStatistical(string data)
        {
            string info = null;
            string names = DateTime.Now.ToString("yyyyMMddhhmmss") + ".csv";
            string path = AppDomain.CurrentDomain.SetupInformation.ApplicationBase + ConfigurationManager.AppSettings["cvspath"];

            string param = "";

            string sql = null;
            if (!string.IsNullOrEmpty(data))
            {
                JObject datajson = JObject.Parse(data);
                string[] mobile = datajson["mobile"].ToString().Replace(" ", ",").Split(',');
                for (int i = 0; i < mobile.Length; i++)
                {
                    if (!string.IsNullOrEmpty(mobile[i].Trim()))
                        param += ",'" + mobile[i].Trim() + "'";
                }
                if (!string.IsNullOrEmpty(param))
                    param = " and mobile in(" + param.Substring(1) + ")";

                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                    DateTime t = DateTime.Parse(datajson["edate"].ToString());
                    DateTime t1 = t.AddDays(1);
                    string eday = t1.ToShortDateString();
                    string sday = DateTime.Parse(datajson["sdate"].ToString()).ToShortDateString();

                    if (datajson["rbauto"].ToString() == "0")
                        param += " and a.datatime between '" + sday + "' and '" + eday + "'";
                    else
                        param += " and a.date between '" + sday + "' and '" + eday + "'";

                }

                if (!string.IsNullOrEmpty(datajson["company"].ToString()))
                {
                    if (Convert.ToInt32(datajson["company"]) > 0)
                        param += " and a.companyID=" + datajson["company"].ToString();
                }
            }

            sql = "SELECT a.mobile as 卡号,a.total as 退费金额,(select count(b.infoid) from [refund] as b where b.mobile=a.mobile) as 扣费次数,"
                +"公司=STUFF((SELECT ','+b.name FROM [refund] t left join company as b on b.infoid=t.companyID WHERE t.mobile=a.mobile group by b.name order by b.name FOR XML PATH('')), 1, 1, '')"
                +"FROM [refund] as a where a.infoid>0 " + param + " group by a.mobile,a.total";

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {
                DataRow dRow = dt.NewRow();
                dRow["卡号"] = dt.Rows.Count;
                dRow["退费金额"] = dt.Compute("sum(退费金额)", "");
                dt.Rows.Add(dRow);  

                bool flag = exportRefund(dt, path + names);
                if (flag)
                    info = names;
            }

            return info;
        }


        public static string getStatisticsIdo(string search)
        {
            string info = null;
            string sql = null;
            string param = "";

            JObject datajson = JObject.Parse(search);
            string stime = datajson["stime"].ToString();
            string etime = datajson["etime"].ToString();

            if (stime.Length < 5)
                datajson["stime"] = "0" + stime;
            if (etime.Length < 5)
                datajson["etime"] = "0" + etime;


            if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
            {

                string eday = datajson["edate"].ToString();
                string[] temp = eday.Split('-');
                if (temp[1].Length < 2)
                    temp[1] = "0" + temp[1];
                if (temp[2].Length < 2)
                    temp[2] = "0" + temp[2];
                datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];
                param += " and a.datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"] + "' and  DATEADD(MINUTE,+1,'" + datajson["edate"].ToString() + " " + datajson["etime"] + "')";

            }
            if (!string.IsNullOrEmpty(datajson["company"].ToString()))
            {
                if (Convert.ToInt32(datajson["company"]) > 0)
                    param += " and a.companyID=" + datajson["company"].ToString();
            }

            if (Convert.ToInt32(datajson["index"]) == 1)
            {
                sql = "select CONVERT(varchar(10),a.datatime,120) AS 日期,"
                     + "count(a.infoid) as 上行总量,"

                     + "sum(case when a.optype=0 then 1 else 0 end) as 成功定购总量,"
                     + "sum(case when a.optype=1 then 1 else 0 end) as 退订总量,"
                     + "sum(case when a.optype=0 then 1 else 0 end)-sum(case when a.optype=1 then 1 else 0 end) as 有效定购,"
                     + "sum(case when a.optype=0 then fee else 0 end)-sum(case when a.optype=fee then 1 else 0 end) as 有效信息费,"
                     + "case when sum(case when a.resultCode=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when a.resultCode=0 then 1 else 0 end))/Convert(decimal(18,2),count(a.infoid))*100) else 0 end as 转化率,"
                     + "sum(case when b.resultCode=0 then 1 else 0 end) as 同步成功总量,"
                     + "case when sum(case when b.resultCode=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when b.resultCode=0 then 1 else 0 end))/ Convert(decimal(18,2),sum(case when a.resultCode=0 then 1 else 0 end))*100) else 0 end as 同步率,"
                     + "sum(case when b.resultCode=0 then b.fee else 0 end) as 同步有效信息费,"
                     + "sum(case when b.result=1 then 1 else 0 end) as 回掉失败"
                     + " from ido_2016 as a WITH(NOLOCK) left join ido_notify as b on a.infoid=b.infoid where a.buyid=1" + param
                     + " group by convert(varchar(10), a.datatime,120) order by convert(varchar(10), a.datatime,120) desc";
            }
            else if (Convert.ToInt32(datajson["index"]) == 2)
            {

                sql = "select a.name as 省份,"
                    + "sum(case when b.buyID=1" + param.Replace("a.", "b.") + " then 1 else 0 end) as 上行总量,"
                    + "count(DISTINCT(case when b.buyID=1" + param.Replace("a.", "b.") + " then b.mobile end)) as 独立用户,"
                    + "sum(case when b.buyID=1" + param.Replace("a.", "b.") + " and b.resultCode=0 then 1 else 0 end) as 成功订购总量,"
                    + "sum(case when b.buyID=1" + param.Replace("a.", "b.") + " and b.resultCode=0 then b.fee else 0 end) as 有效信息费,"
                    + "case when sum(case when b.buyID=1" + param.Replace("a.", "b.") + " and b.resultCode=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when b.buyID=1" + param.Replace("a.", "b.") + " and b.resultCode=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when b.buyID=1" + param.Replace("a.", "b.") + " then 1 else 0 end))*100) else 0 end as 转化率,"
                    + "sum(case when c.buyID=1" + param.Replace("a.", "c.") + " and c.resultCode=0 then 1 else 0 end) 同步成功总量,"
                    + "case when sum(case when c.buyID=1" + param.Replace("a.", "c.") + " and c.resultCode=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.buyID=1" + param.Replace("a.", "c.") + " and c.resultCode=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when b.buyID=1" + param.Replace("a.", "b.") + " and b.resultCode=0 then 1 else 0 end))*100) else 0 end as  同步率,"
                    + "sum(case when c.buyID=1" + param.Replace("a.", "c.") + " and c.resultCode=0 then c.fee else 0 end) as 同步有效信息费,"
                    + "sum(case when c.buyID=1" + param.Replace("a.", "c.") + " and c.result=1 then 1 else 0 end) as 回掉失败"
                    + " from areaInfo as a left join tab_2016 as b WITH(NOLOCK) on SUBSTRING(b.area,0,CHARINDEX(' ',b.area))=a.name left join tab_notify as c on c.infoid=b.infoid where a.parent_id=0 group by a.name,a.sort order by a.sort";

            }
            else
            {
                sql = "select a.name as 公司名称,"
                 + "case when b.fee is null then 0 else b.fee end as 资费,"
                 + "count(b.infoid) as 上行总量,"
                 + "COUNT(DISTINCT(b.mobile)) as 独立用户,"
                 + "sum(case when b.resultCode=0 then 1 else 0 end) as 成功订购总量,"
                 + "sum(case when b.resultCode=0 then b.fee else 0 end) as 有效信息费,"
                 + "case when sum(case when b.resultCode=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when b.resultCode=0 then 1 else 0 end))/Convert(decimal(18,2),count(b.infoid))*100) else 0 end as 转化率,"
                 + "sum(case when b.resultCode=0 then 1 else 0 end) as 同步成功总量,"
                 + "case when sum(case when c.resultCode=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.resultCode=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when b.resultCode=0 then 1 else 0 end))*100) else 0 end as 同步率,"
                 + "sum(case when c.resultCode=0 then c.fee else 0 end) as 同步有效信息费,"
                 + "sum(case when c.result=1 then 1 else 0 end) as 回掉失败"
                 + " from company as a left join tab_2016 as b WITH(NOLOCK) on b.companyID=a.infoid left join tab_notify as c on c.infoid=b.infoid"
                 + " where b.buyid=1" + param.Replace("a.", "b.") + " group by a.name,b.fee order by a.name";
            }

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)

                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";

            return info;
        }

        /*============================================2017新增方法===============================================*/


        public static string getInstantData(int pagesize, int currentpage, string param)
        {
            string info = "{data:[],total:0}";
            string sql = null;
            string paramstr = "";

            if (!string.IsNullOrEmpty(param))
            {
                JObject datajson = JObject.Parse(param);

                string[] items = datajson["items"].ToString().Replace(" ", ",").Split(',');

                for (int i = 0; i < items.Length; i++)
                {
                    if (!string.IsNullOrEmpty(items[i].Trim()))
                        paramstr += ",'" + items[i].Trim() + "'";
                }


                if (!string.IsNullOrEmpty(paramstr.Trim()))
                {
                    if (Convert.ToInt32(datajson["index"]) == 1)
                        paramstr = " and a.imsi in(" + paramstr.Substring(1) + ") ";
                    else if (Convert.ToInt32(datajson["index"]) == 2)
                        paramstr = " and a.mobile in(" + paramstr.Substring(1) + ") ";
                    else if (Convert.ToInt32(datajson["index"]) == 3)
                        paramstr = " and a.userOrder in(" + paramstr.Substring(1) + ") ";
                }

                if (!string.IsNullOrEmpty(datajson["flag"].ToString().Trim()))
                {
                    if (datajson["flag"].ToString().IndexOf("[") == -1)
                         paramstr += " and a.resultCode=" + datajson["flag"].ToString();
                    
                }

                if (null != datajson["area"] && !string.IsNullOrEmpty(datajson["area"].ToString()))
                {
                    string area ="";
                    if (datajson["area"].ToString().IndexOf("[")!= -1)
                    {
                        JArray lsa = JArray.Parse(datajson["area"].ToString());

                        for (int i = 0; i < lsa.Count; i++)
                            area += "or a.area like '" + lsa[i].ToString() + "%' ";

                        paramstr += " and (" + area.Substring(2) + ") ";
                    }
                    else
                        paramstr += "and a.area like '" + datajson["area"].ToString() + "%' ";
                }

                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                    /*DateTime t = DateTime.Parse(datajson["edate"].ToString());
                    DateTime t1 = t.AddDays(1);
                    string eday = t1.ToShortDateString();
                    string sday = DateTime.Parse(datajson["sdate"].ToString()).ToShortDateString();
                    */
                    string stime = datajson["stime"].ToString();
                    string etime = datajson["etime"].ToString();

                    if (stime.Length < 5)
                        datajson["stime"] = "0" + stime;
                    if (etime.Length < 5)
                        datajson["etime"] = "0" + etime;

                    paramstr += " and a.datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"] + "' and  DATEADD(MINUTE,+1,'" + datajson["edate"].ToString() + " " + datajson["etime"] + "')";

                }

                if (!string.IsNullOrEmpty(datajson["conduit"].ToString()))
                {
                    if (Convert.ToInt32(datajson["conduit"]) > 0)
                        paramstr += " and a.conduitID=" + datajson["conduit"].ToString();
                }

                if (!string.IsNullOrEmpty(datajson["company"].ToString()))
                {
                    if (Convert.ToInt32(datajson["company"]) > 0)
                        paramstr += " and a.companyID=" + datajson["company"].ToString();
                }

                if (!string.IsNullOrEmpty(datajson["product"].ToString()))
                {
                    if (Convert.ToInt32(datajson["product"]) > 0)
                        paramstr += " and a.productID=" + datajson["product"].ToString();
                }

            }
            int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "SELECT COUNT(a.infoid) FROM public_notify_2017 as a WITH(NOLOCK) WHERE a.infoid>0" + paramstr));
            if (currentpage == 1)
                sql = "SELECT TOP " + pagesize + " a.infoid,a.companyid,a.productid,a.userorder,b.name,c.name as product,a.mobile,a.imsi,a.fee,a.area,c.conduitid,a.attach,"
                    + "(CASE WHEN a.OPType=0 THEN '定购'  WHEN a.OPType=1 THEN '退订' END) optype,"
                    + "(CASE WHEN a.buyid=1 THEN '点播' ELSE '包月' END) buyid,"
                    + "(CASE WHEN a.resultCode=0 THEN '成功' END) status,"
                    + "(select CASE WHEN count(infoid)>0 THEN 1 END from refund where tablename='public_notify_2017' and dataid=a.infoid) refund,"
                    + "(select CASE WHEN total>0 THEN total END from refund where tablename='public_notify_2017' and dataid=a.infoid) amount,"
                    + "(select CASE WHEN info is not null THEN info END from refund where tablename='public_notify_2017' and dataid=a.infoid) source,"
                    + "(select CASE WHEN note is not null or note<>'' THEN note END from refund where tablename='public_notify_2017' and dataid=a.infoid) note,"
                    + "(select CASE WHEN date is not null THEN  CONVERT(varchar(20),date , 20) END from refund where tablename='public_notify_2017'and dataid=a.infoid) date,"
                    + "(select CASE WHEN count(infoid)>0 THEN 1 END from blacklist where mobile=a.mobile) balck,"
                    + "(select CASE WHEN count(infoid)=1 THEN '已同步' END from public_sync_2017 WITH(NOLOCK) where datatime=a.datatime and infoid=a.infoid) sync,"
                    + "(CASE WHEN len(a.status)>0 then a.status ELSE ''END) as describe,"
                    + "CONVERT(varchar(21), a.datatime, 20) ordered"
                    + " FROM public_notify_2017 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyid"
                    + " right join product as c on c.infoid=a.productID"
                    + " where a.infoid>0 " + paramstr + " ORDER BY a.datatime DESC";
            else
            {
                sql = "SELECT TOP " + pagesize + " a.infoid,a.companyid,a.productid,a.userorder,b.name ,c.name as product,a.mobile,a.imsi,a.fee,a.area,c.conduitid,,a.attach,"
                    + "(CASE WHEN a.OPType=0 THEN '定购'  WHEN a.OPType=1 THEN '退订'  END) optype,"
                    + "(CASE WHEN a.buyid=1 THEN '点播' ELSE '包月' END) buyid,"
                    + "(CASE WHEN a.resultCode=0 THEN '成功' END) status,"
                    + "(select CASE WHEN count(infoid)>0 THEN 1 END from refund where tablename='public_notify_2017' and dataid=a.infoid) refund,"
                    + "(select CASE WHEN total>0 THEN total END from refund where tablename='public_notify_2017' and dataid=a.infoid) amount,"
                    + "(select CASE WHEN info is not null THEN info END from refund where tablename='public_notify_2017' and dataid=a.infoid) source,"
                    + "(select CASE WHEN note is not null or note<>'' THEN note END from refund where tablename='public_notify_2017' and dataid=a.infoid) note,"
                    + "(select CASE WHEN date is not null THEN  CONVERT(varchar(20),date , 20) END from refund where tablename='public_notify_2017' and dataid=a.infoid) date,"
                    + "(select CASE WHEN count(infoid)>0 THEN 1 END from blacklist as b where mobile=a.mobile) balck,"
                    + "(select CASE WHEN count(infoid)=1 THEN '已同步' END from public_sync_2017 WITH(NOLOCK) where datatime=a.datatime and infoid=a.infoid) sync,"
                    + "(CASE WHEN len(a.status)>0 then a.status ELSE ''END) as describe,"
                    + "CONVERT(varchar(21), a.datatime, 20) ordered"
                    +" FROM public_notify_2017 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyid"
                    + " right join product as c on c.infoid=a.productID WHERE a.datatime <=(SELECT MIN(datatime)"
                    + " FROM (SELECT TOP " + ((currentpage - 1) * pagesize + 1) + " [datatime] FROM public_notify_2017 WITH(NOLOCK)"
                    + " WHERE infoid>0" + paramstr.Replace("a.", "") + " ORDER BY [datatime] DESC) AS [tblTmp]) and a.infoid>0 "
                    + paramstr + " ORDER BY a.datatime DESC";
            }
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",total:" + TotalRecords + "}";


            return info;
        }



        public static string getInstantData(int pagesize, int currentpage, string param, string node)
        {
            string info = "{data:[],total:0}";
            string sql = null;
            string paramstr = "";
            string include = "";
            string datatime = null;

            Time tm = new Time();
            datatime = " and a.datatime between " + tm.GetToday();

            if (!string.IsNullOrEmpty(param))
            {
                JObject datajson = JObject.Parse(param);

                string[] items = datajson["items"].ToString().Replace(" ", ",").Split(',');


                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                    string stime = datajson["stime"].ToString();
                    string etime = datajson["etime"].ToString();

                    if (stime.Length < 5)
                        datajson["stime"] = "0" + stime;
                    if (etime.Length < 5)
                        datajson["etime"] = "0" + etime;

                    datatime = " and a.datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"] + "' and  DATEADD(MINUTE,+1,'" + datajson["edate"].ToString() + " " + datajson["etime"] + "')";

                }

                for (int i = 0; i < items.Length; i++)
                {
                    if (!string.IsNullOrEmpty(items[i].Trim()))
                        include += ",'" + items[i].Trim() + "'";
                }

                if (!string.IsNullOrEmpty(include.Trim()))
                {
                    if (Convert.ToInt32(datajson["index"]) == 1)
                        paramstr = " and a.imsi in(" + include.Substring(1) + ") ";
                    else if (Convert.ToInt32(datajson["index"]) == 2)
                        paramstr = " and a.mobile in(" + include.Substring(1) + ") ";
                    else if (Convert.ToInt32(datajson["index"]) == 3)
                        paramstr = " and a.userOrder in(" + include.Substring(1) + ") ";
                }

                if (!string.IsNullOrEmpty(datajson["flag"].ToString().Trim()))
                {
                    if (datajson["flag"].ToString().IndexOf("[") == -1)
                        paramstr += " and a.resultCode=" + datajson["flag"].ToString();
                }

                if (!string.IsNullOrEmpty(datajson["optype"].ToString().Trim()))
                {
                    if (datajson["optype"].ToString().IndexOf("[") == -1)
                        paramstr += " and a.optype=" + datajson["optype"].ToString();
                }

                if (null != datajson["area"] && !string.IsNullOrEmpty(datajson["area"].ToString()))
                {
                    string area = "";
                    if (datajson["area"].ToString().IndexOf("[") != -1)
                    {
                        JArray lsa = JArray.Parse(datajson["area"].ToString());

                        for (int i = 0; i < lsa.Count; i++)
                            area += "or a.area like '" + lsa[i].ToString() + "%' ";

                        paramstr += " and (" + area.Substring(2) + ") ";
                    }
                    else
                        paramstr += "and a.area like '" + datajson["area"].ToString() + "%' ";
                }


                if (!string.IsNullOrEmpty(node))
                {
                    JObject json = JObject.Parse(node);
                    if (json["level"].ToString() == "0")
                        paramstr += " and a.conduitID in (" + json["list"].ToString() + ")";
                    else if (json["level"].ToString() == "1")
                        paramstr += " and a.productID=" + json["infoid"].ToString();
                    else if (json["level"].ToString() == "2")
                        paramstr += " and a.productID=" + json["pid"].ToString() + " and a.companyID=" + json["infoid"].ToString();
                }

            }
            int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "SELECT COUNT(a.infoid) FROM public_notify_2017 as a WITH(NOLOCK) WHERE a.infoid>0" + datatime + paramstr));
            if (currentpage == 1)
                sql = "SELECT TOP " + pagesize + " a.infoid,a.extend3,a.companyid,a.productid,a.userorder,b.name,c.name as product,a.mobile,a.imsi,case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end as fee,a.area,c.conduitid,a.attach,"
                    + "(CASE WHEN a.OPType=0 THEN '定购'  WHEN a.OPType=1 THEN '退订'  END) optype,"
                    + "(CASE WHEN a.buyid=1 THEN '点播' WHEN a.buyid=3 THEN '注册' ELSE '包月' END) buyid,"
                    + "(CASE WHEN a.resultCode=0 THEN '成功' END) status,"
                    + "(select CASE WHEN count(infoid)>0 THEN 1 END from refund where tablename='public_notify_2017' and dataid=a.infoid) refund,"
                    + "(select CASE WHEN total>0 THEN total END from refund where tablename='public_notify_2017' and dataid=a.infoid) amount,"
                    + "(select CASE WHEN info is not null THEN info END from refund where tablename='public_notify_2017' and dataid=a.infoid) source,"
                    + "(select CASE WHEN note is not null or note<>'' THEN note END from refund where tablename='public_notify_2017' and dataid=a.infoid) note,"
                    + "(select CASE WHEN date is not null THEN  CONVERT(varchar(20),date , 20) END from refund where tablename='public_notify_2017'and dataid=a.infoid) date,"
                    + "(select CASE WHEN count(infoid)>0 THEN 1 END from blacklist where mobile=a.mobile) balck,"
                    + "(select CASE WHEN count(infoid)=1 THEN '已同步' END from public_sync_2017 WITH(NOLOCK) where datatime=a.datatime and infoid=a.infoid) sync,"
                    + "(CASE WHEN len(a.status)>0 then a.status ELSE ''END) as describe,"
                    + "CONVERT(varchar(21), a.datatime, 20) ordered"
                    + " FROM public_notify_2017 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyid"
                    + " right join product as c on c.infoid=a.productID"
                    + " where a.infoid>0 " + datatime + paramstr + " ORDER BY a.datatime DESC";
            else
            {
                sql = "SELECT TOP " + pagesize + " a.infoid,a.extend3,a.companyid,a.productid,a.userorder,b.name ,c.name as product,a.mobile,a.imsi,case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end as fee,a.area,c.conduitid,a.attach,"
                    + "(CASE WHEN a.OPType=0 THEN '定购' WHEN a.OPType=1 THEN '退订' END) optype,"
                    + "(CASE WHEN a.buyid=1 THEN '点播' WHEN a.buyid=3 THEN '注册' ELSE '包月' END) buyid,"
                    + "(CASE WHEN a.resultCode=0 THEN '成功' END) status,"
                    + "(select CASE WHEN count(infoid)>0 THEN 1 END from refund where tablename='public_notify_2017' and dataid=a.infoid) refund,"
                    + "(select CASE WHEN total>0 THEN total END from refund where tablename='public_notify_2017' and dataid=a.infoid) amount,"
                    + "(select CASE WHEN info is not null THEN info END from refund where tablename='public_notify_2017' and dataid=a.infoid) source,"
                    + "(select CASE WHEN note is not null or note<>'' THEN note END from refund where tablename='public_notify_2017' and dataid=a.infoid) note,"
                    + "(select CASE WHEN date is not null THEN  CONVERT(varchar(20),date , 20) END from refund where tablename='public_notify_2017' and dataid=a.infoid) date,"
                    + "(select CASE WHEN count(infoid)>0 THEN 1 END from blacklist as b where mobile=a.mobile) balck,"
                    + "(select CASE WHEN count(infoid)=1 THEN '已同步' END from public_sync_2017 WITH(NOLOCK) where datatime=a.datatime and infoid=a.infoid) sync,"
                    + "(CASE WHEN len(a.status)>0 then a.status ELSE ''END) as describe,"
                    + "CONVERT(varchar(21), a.datatime, 20) ordered"
                    + " FROM public_notify_2017 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyid"
                    + " right join product as c on c.infoid=a.productID WHERE a.datatime <=(SELECT MIN(datatime)"
                    + " FROM (SELECT TOP " + ((currentpage - 1) * pagesize + 1) + " [datatime] FROM public_notify_2017 WITH(NOLOCK)"
                    + " WHERE infoid>0" + datatime.Replace("a.", "") + paramstr.Replace("a.", "") + " ORDER BY [datatime] DESC) AS [tblTmp]) and a.infoid>0 "
                    + datatime + paramstr + " ORDER BY a.datatime DESC";
            }
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",total:" + TotalRecords + "}";


            return info;
        }




        /// <summary>
        /// 数据错误统计
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        public static string getStatisticsData(string param, string tabname, string code, string typeid)
        {
            string info = "{data:[],total:0}";
            string sql = null;
            string paramstr = "";
            string field = "mobile";
            string tab = "public_order_2017";

            if (tabname == "1")
                tab = "public_notify_2017";



            if (!string.IsNullOrEmpty(param))
            {
                JObject datajson = JObject.Parse(param);

                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                    /*DateTime t = DateTime.Parse(datajson["edate"].ToString());
                    DateTime t1 = t.AddDays(1);
                    string eday = t1.ToShortDateString();
                    string sday = DateTime.Parse(datajson["sdate"].ToString()).ToShortDateString();
                    */
                    string stime = datajson["stime"].ToString();
                    string etime = datajson["etime"].ToString();

                    if (stime.Length < 5)
                        datajson["stime"] = "0" + stime;
                    if (etime.Length < 5)
                        datajson["etime"] = "0" + etime;

                    paramstr += " and datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"] + "' and  DATEADD(MINUTE,+1,'" + datajson["edate"].ToString() + " " + datajson["etime"] + "')";

                   
                }

                if (!string.IsNullOrEmpty(datajson["conduit"].ToString()))
                {
                    if (Convert.ToInt32(datajson["conduit"]) > 0)
                        paramstr += " and conduitID=" + datajson["conduit"].ToString();
                }

                if (!string.IsNullOrEmpty(datajson["company"].ToString()))
                {
                    if (Convert.ToInt32(datajson["company"]) > 0)
                        paramstr += " and companyID=" + datajson["company"].ToString();
                }

                if (!string.IsNullOrEmpty(datajson["product"].ToString()))
                {
                    if (Convert.ToInt32(datajson["product"]) > 0)
                        paramstr += " and productID=" + datajson["product"].ToString();
                }

                if (!string.IsNullOrEmpty(code))
                {
                    if (code == "1")
                        paramstr += " and codeflag=1 ";
                    else
                        paramstr += " and codeflag=0 ";
                }

                if (!string.IsNullOrEmpty(datajson["index"].ToString()))
                {
                    if (Convert.ToInt32(datajson["index"])==1)
                        field = "IMSI";
                    else if(Convert.ToInt32(datajson["index"])==3)
                        field = "userOrder";
                }

                if (!string.IsNullOrEmpty(typeid))
                {
                    if (!string.IsNullOrEmpty(datajson["area"].ToString()))
                        paramstr += " and area like '" + datajson["area"].ToString() + "%'";
                }
            }

            sql = "select number,status,Convert(decimal(18,2),Convert(decimal(18,2),number)/Convert(decimal(18,2),total))*100 as ratios from(SELECT COUNT(" + field + ") as number,status,(select count(infoid) from " + tab + " where infoid>0 " + paramstr + ") as total FROM " + tab + " as a where infoid>0 " + paramstr + " group by status) tab";

            //sql = "SELECT number,status,Convert(decimal(18,2),Convert(decimal(18,2),number)/Convert(decimal(18,2),sum(number))) as ratios SELECT COUNT(" + field + ") as number,status FROM " + tab
                    //+ " where infoid>0 " + paramstr + " group by status";
            
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter())+ "}";


            return info;
        }


        public static string getStatisticsData(string param, string node, string typeid)
        {
            string info = "{data:[],total:0}";
            string sql = null;
            string paramstr = "";
            string tab = "public_order_2017 WITH(NOLOCK)";
            string column = "status";

            if (!string.IsNullOrEmpty(param))
            {
                JObject datajson = JObject.Parse(param);

                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                   
                    string stime = datajson["stime"].ToString();
                    string etime = datajson["etime"].ToString();

                    if (stime.Length < 5)
                        datajson["stime"] = "0" + stime;
                    if (etime.Length < 5)
                        datajson["etime"] = "0" + etime;

                    paramstr += "datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"] + "' and  DATEADD(MINUTE,+1,'" + datajson["edate"].ToString() + " " + datajson["etime"] + "')";
                }

                if (null != datajson["verifycode"])
                {
                    if (datajson["verifycode"].ToString().IndexOf("[") == -1)
                        paramstr += " and codeflag=" + datajson["verifycode"].ToString();
                }
                else
                {
                    tab = "public_notify_2017 WITH(NOLOCK)";
                    column = datajson["field"].ToString();
                }
                if (!string.IsNullOrEmpty(node))
                {
                    JObject json = JObject.Parse(node);
                    if (json["level"].ToString() == "0")
                        paramstr += " and conduitID in (" + json["list"].ToString() + ")";
                    else if (json["level"].ToString() == "1")
                        paramstr += " and productID=" + json["infoid"].ToString();
                    else if (json["level"].ToString() == "2")
                        paramstr += " and productID=" + json["pid"].ToString() + " and companyID=" + json["infoid"].ToString();
                }

                if (!string.IsNullOrEmpty(typeid))
                {
                    if (null != datajson["area"])
                    {
                        if (datajson["area"].ToString().IndexOf("[") == -1)
                            paramstr += " and area like '" + datajson["area"].ToString() + "%'";
                    }
                }
               
            }

            //sql = "select number,status,Convert(decimal(18,2),Convert(decimal(18,2),number)/Convert(decimal(18,2),total))*100 as ratios from(SELECT COUNT(infoid) as number,Replace(status,mobile,''),(select count(infoid) from " + tab + " where " + paramstr + ") as total FROM " + tab + " where " + paramstr + " group by status) tab";

            if (column == "extend3")
               sql = "select number,status,Convert(decimal(18,2),Convert(decimal(18,2),number)/Convert(decimal(18,2),total))*100 as ratios from(SELECT count(extend3) as number,extend3 as status,(select count(infoid) from " + tab + " where " + paramstr + ") as total FROM " + tab + " where " + paramstr + " group by extend3) as tab";
            else
               sql = "select number,status,Convert(decimal(18,2),Convert(decimal(18,2),number)/Convert(decimal(18,2),total))*100 as ratios from(select sum(number) as number,status,(select count(infoid) from " + tab + " where " + paramstr + ") as total from(SELECT count(infoid) as number,Replace(status,SUBSTRING(status,CHARINDEX('[',status),CHARINDEX(']',status)),'') as status FROM " + tab + " where " + paramstr + " group by status) as tabs group by tabs.status) tab";
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";


            return info;
        }



        /// <summary>
        /// 获取请求数据
        /// </summary>
        /// <param name="pagesize"></param>
        /// <param name="currentpage"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static string getDataRequest(int pagesize, int currentpage, string param)
        {
            string info = "{data:[],total:0}";
            string sql = null;
            string paramstr = "";

            if (!string.IsNullOrEmpty(param))
            {
                JObject datajson = JObject.Parse(param);

                string[] items = datajson["items"].ToString().Replace(" ", ",").Split(',');
             
                for (int i = 0; i < items.Length; i++)
                {
                    if (!string.IsNullOrEmpty(items[i].Trim()))
                        paramstr += ",'" + items[i].Trim() + "'";
                }

                if (!string.IsNullOrEmpty(paramstr))
                {
                    if (Convert.ToInt32(datajson["index"]) == 1)
                        paramstr = " and a.imsi in(" + paramstr.Substring(1) + ") ";
                    else if(Convert.ToInt32(datajson["index"]) == 2)
                        paramstr = " and a.mobile in(" + paramstr.Substring(1) + ") ";
                    else if(Convert.ToInt32(datajson["index"]) == 3)
                        paramstr = " and a.userOrder in(" + paramstr.Substring(1) + ") ";
                }

                if (null!=datajson["flag"])
                {
                    if (datajson["flag"].ToString().IndexOf("[") == -1)
                        paramstr += " and a.resultCode=" + datajson["flag"].ToString();

                }

                if (null!=datajson["verifycode"])
                {
                    if (datajson["verifycode"].ToString().IndexOf("[") == -1)
                        paramstr += " and a.codeflag=" + datajson["verifycode"].ToString();
                }


                if (null != datajson["area"] && !string.IsNullOrEmpty(datajson["area"].ToString()))
                {
                    string area = "";
                    if (datajson["area"].ToString().IndexOf("[") != -1)
                    {
                        JArray lsa = JArray.Parse(datajson["area"].ToString());

                        for (int i = 0; i < lsa.Count; i++)
                            area += "or a.area like '" + lsa[i].ToString() + "%' ";

                        paramstr += " and (" + area.Substring(2) + ") ";
                    }
                    else
                        paramstr += "and a.area like '" + datajson["area"].ToString() + "%' ";
                }


                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                    /*DateTime t = DateTime.Parse(datajson["edate"].ToString());
                    DateTime t1 = t.AddDays(1);
                    string eday = t1.ToShortDateString();
                    string sday = DateTime.Parse(datajson["sdate"].ToString()).ToShortDateString();
                    */
                    string stime = datajson["stime"].ToString();
                    string etime = datajson["etime"].ToString();

                    if (stime.Length < 5)
                        datajson["stime"] = "0" + stime;
                    if (etime.Length < 5)
                        datajson["etime"] = "0" + etime;

                    paramstr += " and a.datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"] + "' and  DATEADD(MINUTE,+1,'" + datajson["edate"].ToString() + " " + datajson["etime"] + "')";

                }

                if (!string.IsNullOrEmpty(datajson["conduit"].ToString()))
                {
                    if (Convert.ToInt32(datajson["conduit"]) > 0)
                        paramstr += " and a.conduitID=" + datajson["conduit"].ToString();
                }

                if (!string.IsNullOrEmpty(datajson["company"].ToString()))
                {
                    if (Convert.ToInt32(datajson["company"]) > 0)
                        paramstr += " and a.companyID=" + datajson["company"].ToString();
                }

                if (!string.IsNullOrEmpty(datajson["product"].ToString()))
                {
                    if (Convert.ToInt32(datajson["product"]) > 0)
                        paramstr += " and a.productID=" + datajson["product"].ToString();
                }

            }
            int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "SELECT COUNT(a.infoid) FROM public_order_2017 as a WITH(NOLOCK) WHERE a.infoid>0" + paramstr));
            if (currentpage == 1)
            {
                sql = "SELECT TOP " + pagesize + " a.infoid,b.name,c.name as product,a.productid,a.companyid,a.conduitid,a.userorder,a.mobile,case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end as fee,a.area,a.imsi,a.status,a.code,a.attach,"
                    + "(CASE WHEN a.OPType=0 THEN '定购' WHEN a.OPType=1 THEN '退订' END) optype,"
                    + "(CASE WHEN a.buyid=1 THEN '点播' WHEN a.buyid=3 THEN '注册' ELSE '包月' END) buyid,"
                    + "(CASE WHEN a.resultCode=0 THEN '成功' END) resultCode,"
                    + "CONVERT(varchar(21), a.datatime, 20) ordered"
                    + " FROM public_order_2017 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyid"
                    + " left join product as c on c.infoid=a.productID"
                    + " where a.infoid>0 " + paramstr + " ORDER BY a.datatime DESC";
                
            }
            else
            {
                sql = "SELECT TOP " + pagesize + " a.infoid,b.name,c.name as product,a.productid,a.companyid,a.conduitid,a.userorder,a.mobile,case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end as fee,a.area,a.imsi,a.status,a.code,a.attach,"
                    + "(CASE WHEN a.OPType=0 THEN '定购' WHEN a.OPType=1 THEN '退订' END) optype,"
                    + "(CASE WHEN a.buyid=1 THEN '点播' WHEN a.buyid=3 THEN '注册' ELSE '包月' END) buyid,"
                    + "(CASE WHEN a.resultCode=0 THEN '成功' END) resultCode,"
                    + "CONVERT(varchar(21), a.datatime, 20) ordered"
                    + " FROM public_order_2017 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyid"
                    + " left join product as c on c.infoid=a.productID WHERE a.datatime<=(SELECT MIN(datatime)"
                    + " FROM (SELECT TOP " + ((currentpage - 1) * pagesize + 1) + " [datatime] FROM public_order_2017 WITH(NOLOCK)"
                    + " WHERE infoid>0" + paramstr.Replace("a.", "") + " ORDER BY [datatime] desc) AS [tblTmp]) and a.infoid>0 "
                    + paramstr + " ORDER BY a.datatime desc";
            }
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",total:" + TotalRecords + "}";


            return info;
        }


        /// <summary>
        /// 获取请求数据列表
        /// </summary>
        /// <param name="pagesize"></param>
        /// <param name="currentpage"></param>
        /// <param name="param"></param>
        /// <param name="node"></param>
        /// <returns></returns>
        public static string getDataRequest(int pagesize, int currentpage, string param, string node)
        {
            string info = "{data:[],total:0}";
            string sql = null;
            string paramstr = "";
            string include = "";
            string datatime = null;
            
             Time tm= new Time();
             datatime =" and a.datatime between "+tm.GetToday();

            if (!string.IsNullOrEmpty(param))
            {
                JObject datajson = JObject.Parse(param);

                string[] items = datajson["items"].ToString().Replace(" ", ",").Split(',');


                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                    /*DateTime t = DateTime.Parse(datajson["edate"].ToString());
                    DateTime t1 = t.AddDays(1);
                    string eday = t1.ToShortDateString();
                    string sday = DateTime.Parse(datajson["sdate"].ToString()).ToShortDateString();
                    */
                    string stime = datajson["stime"].ToString();
                    string etime = datajson["etime"].ToString();

                    if (stime.Length < 5)
                        datajson["stime"] = "0" + stime;
                    if (etime.Length < 5)
                        datajson["etime"] = "0" + etime;

                    datatime = " and a.datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"] + "' and  DATEADD(MINUTE,+1,'" + datajson["edate"].ToString() + " " + datajson["etime"] + "')";

                }

                for (int i = 0; i < items.Length; i++)
                {
                    if (!string.IsNullOrEmpty(items[i].Trim()))
                        include += ",'" + items[i].Trim() + "'";
                }

                if (!string.IsNullOrEmpty(include))
                {
                    if (Convert.ToInt32(datajson["index"]) == 1)
                        paramstr = " and a.imsi in(" + include.Substring(1) + ") ";
                    else if (Convert.ToInt32(datajson["index"]) == 2)
                        paramstr = " and a.mobile in(" + include.Substring(1) + ") ";
                    else if (Convert.ToInt32(datajson["index"]) == 3)
                        paramstr = " and a.userOrder in(" + include.Substring(1) + ") ";
                }

                if (null != datajson["flag"])
                {
                    if (datajson["flag"].ToString().IndexOf("[") == -1)
                    {
                        if (datajson["flag"].ToString()=="0")
                            paramstr += " and a.resultCode=" + datajson["flag"].ToString();
                        else
                            paramstr += " and a.resultCode<>0";

                    }
                }

                if (null != datajson["verifycode"])
                {
                    if (datajson["verifycode"].ToString().IndexOf("[") == -1)
                        paramstr += " and a.codeflag=" + datajson["verifycode"].ToString();
                }


                if (null != datajson["area"] && !string.IsNullOrEmpty(datajson["area"].ToString()))
                {
                    string area = "";
                    if (datajson["area"].ToString().IndexOf("[") != -1)
                    {
                        JArray lsa = JArray.Parse(datajson["area"].ToString());

                        for (int i = 0; i < lsa.Count; i++)
                            area += "or a.area like '" + lsa[i].ToString() + "%' ";

                        paramstr += " and (" + area.Substring(2) + ") ";
                    }
                    else
                        paramstr += "and a.area like '" + datajson["area"].ToString() + "%' ";
                }
            }

            if (!string.IsNullOrEmpty(node))
            {
                JObject json = JObject.Parse(node);
                if(json["level"].ToString()=="0")
                    paramstr += " and a.conduitID in (" + json["list"].ToString()+")";
                else if(json["level"].ToString()=="1")
                    paramstr += " and a.productID=" + json["infoid"].ToString();
                else if (json["level"].ToString() == "2")
                    paramstr += " and a.productID=" + json["pid"].ToString() + " and a.companyID=" + json["infoid"].ToString();
            }
            int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "SELECT COUNT(a.infoid) FROM public_order_2017 as a WITH(NOLOCK) WHERE a.infoid>0" + datatime+paramstr));
            if (currentpage == 1)
            {
                sql = "SELECT TOP " + pagesize + " a.infoid,a.conduitid,a.productid,a.companyid,a.userorder,b.name,c.name as product,a.mobile,case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end as fee,a.area,a.imsi,a.status,a.code,a.attach,"
                    + "(CASE WHEN a.OPType=0 THEN '定购' WHEN a.OPType=1 THEN '退订' END) optype,"
                    + "(CASE WHEN a.buyid=1 THEN '点播' WHEN a.buyid=3 THEN '注册' ELSE '包月' END) buyid,"
                    + "(CASE WHEN a.resultCode=0 THEN '成功' END) resultCode,"
                    + "CONVERT(varchar(21), a.datatime, 20) ordered"
                    + " FROM public_order_2017 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyid"
                    + " left join product as c on c.infoid=a.productID"
                    + " where a.infoid>0 " + datatime+paramstr + " ORDER BY a.datatime DESC";
            }
            else
            {
                sql = "SELECT TOP " + pagesize + " a.infoid,a.conduitid,a.productid,a.companyid,a.userorder,b.name,c.name as product,a.mobile,case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end as fee,a.area,a.imsi,a.status,a.code,a.attach,"
                    + "(CASE WHEN a.OPType=0 THEN '定购'  WHEN a.OPType=1 THEN '退订' END) optype,"
                    + "(CASE WHEN a.buyid=1 THEN '点播' WHEN a.buyid=3 THEN '注册' ELSE '包月' END) buyid,"
                    + "(CASE WHEN a.resultCode=0 THEN '成功' END) resultCode,"
                    + "CONVERT(varchar(21), a.datatime, 20) ordered"
                    + " FROM public_order_2017 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyid"
                    + " left join product as c on c.infoid=a.productID WHERE a.datatime<=(SELECT MIN(datatime)"
                    + " FROM (SELECT TOP " + ((currentpage - 1) * pagesize + 1) + " [datatime] FROM public_order_2017 WITH(NOLOCK)"
                    + " WHERE infoid>0" + datatime.Replace("a.","")+paramstr.Replace("a.", "") + " ORDER BY [datatime] desc) AS [tblTmp]) and a.infoid>0 "
                    + datatime + paramstr + " ORDER BY a.datatime desc";
            }
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",total:" + TotalRecords + "}";


            return info;
        }

        /// <summary>
        /// 获取请求数据明细
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static string getRequestInfo(int infoid)
        {
            string info =string.Empty;
            string sql = "SELECT TOP 1 b.name as company,c.names as conduit,a.productid,a.companyid,a.conduitid,a.uppack,a.fee,a.dowpack,a.imsi,a.status,a.code,a.attach,"
                     + "(CASE WHEN a.buyid=1 THEN '点播' WHEN a.buyid=3 THEN '注册' ELSE '包月' END) buyname,"
                     + "CONVERT(varchar(21), a.datatime, 20) datatime "
                     + "FROM public_order_2017 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyid "
                     + "left join conduit as c on c.infoid=a.conduitID "
                     + "where a.infoid="+infoid;

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{success:true,data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "") + "}";


            return info;
        }


        public static string getInstanDataInfo(int infoid)
        {
            string info = string.Empty;
            string sql = "SELECT TOP 1 b.name as company,c.names as conduit,a.productid,a.companyid,a.conduitid,a.notifypack,a.extend3,case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else a.fee end,(select top 1 syncpack from public_sync_2017 where infoid=a.infoid) as syncpack,a.status,a.attach,"
                     + "(CASE WHEN a.buyid=1 THEN '点播' WHEN a.buyid=3 THEN '注册' ELSE '包月' END) buyname,"
                     + "CONVERT(varchar(21), a.datatime, 20) datatime "
                     + "FROM public_notify_2017 as a WITH(NOLOCK) "
                     + "left join company as b on b.infoid=a.companyid "
                     + "left join conduit as c on c.infoid=a.conduitID "
                     //+ "right join public_sync_2017 as d on d.infoid=a.infoid "
                     + "where a.infoid=" + infoid;

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{success:true,data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "") + "}";


            return info;
        }


        /// <summary>
        /// 查询统计
        /// </summary>
        /// <param name="search"></param>
        /// <returns></returns>
        public static string getMultiServiceData(string search)
        {
            string info = null;
            string sql = null;
            string sql1 = null;
            string param = "";
            string param1 = "";
            string param2 = "";
            JObject datajson = JObject.Parse(search);
            string stime = datajson["stime"].ToString();
            string etime = datajson["etime"].ToString();
            DataTable dt = new DataTable();
            if (stime.Length < 5)
                datajson["stime"] = "0" + stime;
            if (etime.Length < 5)
                datajson["etime"] = "0" + etime;


            if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
            {

                string eday = datajson["edate"].ToString();
                string[] temp = eday.Split('-');
                if (temp[1].Length < 2)
                    temp[1] = "0" + temp[1];
                if (temp[2].Length < 2)
                    temp[2] = "0" + temp[2];
                datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];
                param += " and a.datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"] + "' and  DATEADD(MINUTE,+1,'" + datajson["edate"].ToString() + " " + datajson["etime"] + "')";
                param2 = param;

            }

            if (!string.IsNullOrEmpty(datajson["conduit"].ToString()))
            {
                if (Convert.ToInt32(datajson["conduit"]) > 0)
                {
                    param += " and a.conduitID=" + datajson["conduit"].ToString();
                    param1 += " and a.conduitID=" + datajson["conduit"].ToString();
                    param2 += " and a.conduitID=" + datajson["conduit"].ToString();
                }
            }

            if (!string.IsNullOrEmpty(datajson["company"].ToString()))
            {
                if (Convert.ToInt32(datajson["company"]) > 0)
                {
                    param += " and a.companyID=" + datajson["company"].ToString();
                    param1 += " and a.companyID=" + datajson["company"].ToString();
                }
            }

            if (!string.IsNullOrEmpty(datajson["product"].ToString()))
            {
                if (Convert.ToInt32(datajson["product"]) > 0)
                {
                    param += " and a.productID=" + datajson["product"].ToString();
                    param1 += " and a.productID=" + datajson["product"].ToString();
                    param2 += " and a.productID=" + datajson["product"].ToString();
                }
            }

            if (Convert.ToInt32(datajson["index"]) == 1)
            {

                sql = "select convert(char(10),a.datatime,120) as 日期,"
                      + "0 as 上行总量,"
                      + "0 as 验证码总量,"
                      + "0 as 独立验证码,"
                      + "0 as hours,"
                      + "sum(case when a.optype<>1 then 1 else 0 end) as 通知订购总量,"
                      + "COUNT(DISTINCT(case when a.optype<>1 then (case when len(a.mobile)>0 then a.mobile else a.imsi end) end)) as 独立用户,"
                      + "sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end) as 成功订购总量,"
                      + "sum(case when a.resultCode=0 and a.optype=1 then 1 else 0 end) as 退订总量,"
                      + "sum(case when a.resultCode=0 and a.optype=0 then a.fee else 0 end) as 有效信息费,"
                      + "sum(case when c.resultCode=0 and c.optype=1 then 1 else 0 end) as 同步退订总量,"
                      + "sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end) as 同步成功总量,"
                      + "case when sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when a.optype<>1 then 1 else 0 end))*100) else 0 end as 转化率,"
                      + "case when sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end))/ Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))*100) else 0 end as 同步率,"
                      + "sum(case when c.resultCode=0 and c.optype=0 and c.result=0 then c.fee else 0 end) as 同步有效信息费,"
                      + "sum(case when c.result=1 and c.optype=0 then 1 else 0 end) as 回掉失败 "
                      + "from public_notify_2017 as a WITH(NOLOCK) "
                      + "left join public_sync_2017 as c WITH(NOLOCK) on c.infoid=a.infoid "
                      + "where a.infoid>0" + param
                      + " group by convert(char(10),a.datatime,120)";

               /* sql = "select 日期,sum(上行总量) 上行总量,sum(验证码总量) 验证码总量,sum(hours) hours,"
                      +"sum(通知订购总量) 通知订购总量,sum(独立用户) 独立用户,sum(成功订购总量) 成功订购总量,"
                      +"sum(退订总量) 退订总量,sum(有效信息费) 有效信息费,sum(同步退订总量) 同步退订总量,"
                      +"sum(同步成功总量) 同步成功总量,sum(转化率) 转化率,sum(同步率) 同步率,"
                      +"sum(同步有效信息费) 同步有效信息费,sum(回掉失败) 回掉失败 "
                      +"from(select convert(char(10),a.datatime,120) as 日期,"
                      + "0 as 上行总量,"
                      + "0 as 验证码总量,"
                      + "0 as hours,"
                      + "sum(case when a.optype<>1 then 1 else 0 end) as 通知订购总量,"
                      + "COUNT(DISTINCT(case when a.optype<>1 then a.mobile end)) as 独立用户,"
                      + "COUNT(DISTINCT(case when a.resultCode=0 and a.optype=0 and a.buyid=2 then a.mobile end))+sum(case when a.resultCode=0 and a.optype=0 and a.buyid=1 then 1 else 0 end) as 成功订购总量,"
                      //+ "sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end) as 成功订购总量,"
                      + "sum(case when a.resultCode=0 and a.optype=1 then 1 else 0 end) as 退订总量,"
                      + "sum(case when a.resultCode=0 and a.optype=0 and a.buyID=1 then a.fee else 0 end)+(COUNT(DISTINCT(case when a.resultCode=0 and a.optype=0 and a.buyid=2 then a.mobile end))*a.fee) as 有效信息费,"
                      //+ "sum(case when a.resultCode=0 and a.optype=0 then a.fee else 0 end) as 有效信息费,"
                      + "sum(case when c.resultCode=0 and c.optype=1 then 1 else 0 end) as 同步退订总量,"
                      + "sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end) as 同步成功总量,"
                      + "case when sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when a.optype<>1 then 1 else 0 end))*100) else 0 end as 转化率,"
                      + "case when sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end))/ Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))*100) else 0 end as 同步率,"
                      + "sum(case when c.resultCode=0 and c.optype=0 then c.fee else 0 end) as 同步有效信息费,"
                      + "sum(case when c.result=1 and c.optype=0 then 1 else 0 end) as 回掉失败 "
                      + "from public_notify_2017 as a WITH(NOLOCK) "
                      + "left join public_sync_2017 as c WITH(NOLOCK) on c.infoid=a.infoid "
                      + "where a.infoid>0" + param
                      + " group by a.fee,convert(char(10),a.datatime,120)) as tab group by 日期";*/

                /*string sqla = "WITH x1 AS ("
                   + "select convert(char(10),datatime,23) as 日期,COUNT(DISTINCT(userOrder)) as 验证码总量,0 as 提交总量,count(userOrder) as code from public_order_2017 WITH(NOLOCK) where len(code)>0" + param.Replace("a.", "") + " group by convert(char(10),datatime,23)"
                + "),"
                + "x2 AS ("
                   + "select convert(char(10),datatime,23) as 日期,COUNT(userOrder) as 提交总量,0 验证码总量,0 as code from public_order_2017 WITH(NOLOCK) where infoid>0" + param.Replace("a.", "") + " group by convert(char(10),datatime,23)"
                + ") "
                + "Select  日期,sum(验证码总量) as 验证码总量,sum(提交总量)-sum(code) as 上行总量 from "
                + "("
                + "Select 日期,验证码总量,提交总量,code from  x1 "
                + "Union "
                + "Select  日期,验证码总量,提交总量,code from x2 "
                + ") as a group by 日期";
                 +") "
                 + "select  x0.日期,(x3.提交总量-x3.code) as 上行总量,x3.验证码总量,x0.通知订购总量,x0.独立用户,x0.成功订购总量,x0.退订总量,0 as hours,x0.有效信息费,"
                 +"(case when 验证码总量>0 then "
                 +"Convert(decimal(18,2),Convert(decimal(18,2),成功订购总量)/Convert(decimal(18,2),(验证码总量+(通知订购总量-成功订购总量)))) "
                 +"else " 
                 +"Convert(decimal(18,2),Convert(decimal(18,2),成功订购总量)/Convert(decimal(18,2),通知订购总量)) end)*100 as 转化率,"
                 +"x0.同步退订总量,x0.同步成功总量,x0.同步率,x0.同步有效信息费,x0.回掉失败 from x0 left join x3 on x0.日期=x3.日期 order by x0.日期 desc";*/


                string sqla = "select convert(char(10),datatime,23) as 日期,sum(case when code is null or code='' then 1 else 0 end) as 上行总量,COUNT(DISTINCT(case when buyid=2 and len(IMSI)>0 and len(code)>0 then IMSI when buyid=2 and len(mobile)>0 and len(code)>0 then mobile end))+COUNT(case when buyid=1 and len(IMSI)>0 and len(code)>0 then IMSI when buyid=1 and len(mobile)>0 and len(code)>0 then mobile end) as 独立验证码, COUNT(case when len(code)>0 and len(IMSI)>0 then IMSI when len(code)>0 and len(mobile)>0 then mobile end) as 验证码总量 from public_order_2017 WITH(NOLOCK) where infoid>0" + param.Replace("a.", "") + " group by convert(char(10),datatime,23)";

                 DataTable dt1 = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sqla).Tables[0];

                 sql1 = "select count(a.infoid) from public_notify_2017 as a WITH(NOLOCK) where (select count(e.infoid) from public_notify_2017 as e WITH(NOLOCK) where e.resultCode=0 and e.buyID=2 and e.optype=1 and e.mobile=a.mobile and CONVERT(varchar(30),e.datatime,120)<CONVERT(varchar(30),DATEADD(hh,+72,a.datatime),120)" + param1.Replace("a.", "e.") + ")>0 and a.resultCode=0 and a.buyID=2 and a.optype=0 and a.datatime between '{0}' and CONVERT(varchar(10),DATEADD(hh,+24,'{0}'),120)" + param1;
      
                 dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

                 if (dt1.Rows.Count > 0)
                 {
                     for (int i = 0; i < dt1.Rows.Count; i++)
                     {
                    
                         DataRow[] temp1 = dt.Select("日期='" + dt1.Rows[i]["日期"].ToString() + "'");

                         if (temp1.Length > 0)
                         {
                             temp1[0]["上行总量"] = Convert.ToInt32(dt1.Rows[i]["上行总量"]);
                             temp1[0]["验证码总量"] = Convert.ToInt32(dt1.Rows[i]["验证码总量"]);
                             temp1[0]["独立验证码"] = Convert.ToInt32(dt1.Rows[i]["独立验证码"]);
                             double code = Convert.ToDouble(dt1.Rows[i]["独立验证码"]);
                             double order = Convert.ToDouble(dt1.Rows[i]["上行总量"]);
                             double notify = Convert.ToDouble(temp1[0]["通知订购总量"]);
                             double result = Convert.ToDouble(temp1[0]["成功订购总量"]);

                             if (code > 0)
                             {
                                 double num1 = (result / code);
                                 temp1[0]["转化率"] = Math.Round(num1 * 100, 2);
                             }
                           
                         }
                         else
                         {
                             DataRow dRow = dt.NewRow();
                             dRow["日期"] = dt1.Rows[i]["日期"].ToString();
                             dRow["上行总量"] = Convert.ToInt32(dt1.Rows[i]["上行总量"]);
                             dRow["验证码总量"] = Convert.ToInt32(dt1.Rows[i]["验证码总量"]);
                             dRow["独立验证码"] = Convert.ToInt32(dt1.Rows[i]["独立验证码"]);
                             dt.Rows.Add(dRow);
                         }


                     }
                 }
                 dt.DefaultView.Sort = "日期 DESC";
                 dt = dt.DefaultView.ToTable();

                 foreach (DataRow dr in dt.Rows)
                 {
                     dr["hours"] = get72Hours(string.Format(sql1,dr["日期"].ToString()));
                 }

                
            }
            else if (Convert.ToInt32(datajson["index"]) == 2)
            {
                sql = "WITH x4 AS ("
                    + "select (case when SUBSTRING(a.area,0,CHARINDEX(' ',a.area)) is null then '[无号码]' when SUBSTRING(a.area,0,CHARINDEX(' ',a.area))='' then '[未识别]' else SUBSTRING(a.area,0,CHARINDEX(' ',a.area)) end) as 省份,"
                    + "0 as 上行总量,"
                    + "0 as 验证码总量,"
                    + "0 as 独立验证码,"
                    + "0 as hours,"
                    + "sum(case when a.optype<>1 then 1 else 0 end) as 通知订购总量,"
                    + "COUNT(DISTINCT(case when a.optype<>1 then (case when len(a.mobile)>0 then a.mobile else a.imsi end) end)) as 独立用户,"
                    + "sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end) as 成功订购总量,"
                    + "sum(case when a.resultCode=0 and a.optype=1 then 1 else 0 end) as 退订总量,"
                    + "sum(case when a.resultCode=0 and a.optype=0 then a.fee else 0 end) as 有效信息费,"
                    + "case when sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when a.optype<>1 then 1 else 0 end))*100) else 0 end as 转化率,"
                    + "sum(case when c.resultCode=0 and c.optype=1 then 1 else 0 end) as 同步退订总量,"
                    + "sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end) as 同步成功总量,"
                    + "case when sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))*100) else 0 end as 同步率,"
                    + "sum(case when c.resultCode=0 and c.optype=0 and c.result=0 then c.fee else 0 end) as 同步有效信息费,"
                    + "sum(case when c.result=1 and c.optype=0 then 1 else 0 end) as 回掉失败 "
                    + "from public_notify_2017 as a WITH(NOLOCK) "
                    + "left join public_sync_2017 as c WITH(NOLOCK) on c.infoid=a.infoid "
                    + "where a.infoid>0" + param
                    + " group by SUBSTRING(a.area,0,CHARINDEX(' ',a.area))"
                    + ") "
                    + "select a.name as 省份,x4.上行总量,x4.验证码总量,x4.独立验证码,x4.通知订购总量,x4.独立用户,x4.成功订购总量,x4.退订总量,x4.hours,x4.有效信息费,x4.转化率,x4.同步退订总量,x4.同步成功总量,x4.同步率,x4.同步有效信息费,x4.回掉失败 from areaInfo as a left join x4 on x4.省份=a.name where a.parent_id=0 order by a.sort";

                /*sql = "WITH x4 AS ("
                    + "select (case when SUBSTRING(a.area,0,CHARINDEX(' ',a.area)) is null then '[无号码]' when SUBSTRING(a.area,0,CHARINDEX(' ',a.area))='' then '[未识别]' else SUBSTRING(a.area,0,CHARINDEX(' ',a.area)) end) as 省份,"
                    + "0 as 上行总量,"
                    + "0 as 验证码总量,"
                    + "0 as hours,"
                    + "sum(case when a.optype<>1 then 1 else 0 end) as 通知订购总量,"
                    + "COUNT(DISTINCT(case when a.optype<>1 then a.mobile end)) as 独立用户,"
                    //+ "sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end) as 成功订购总量,"
                    + "COUNT(DISTINCT(case when a.resultCode=0 and a.optype=0 and a.buyid=2 then a.mobile end))+sum(case when a.resultCode=0 and a.optype=0 and a.buyid=1 then 1 else 0 end) as 成功订购总量,"
                    + "sum(case when a.resultCode=0 and a.optype=1 then 1 else 0 end) as 退订总量,"
                    //+ "sum(case when a.resultCode=0 and a.optype=0 then a.fee else 0 end) as 有效信息费,"
                    + "sum(case when a.resultCode=0 and a.optype=0 and a.buyid=1 then a.fee else 0 end)+COUNT(DISTINCT(case when a.resultCode=0 and a.optype=0 and a.buyID=2 then a.mobile end))*a.fee as 有效信息费,"
                    + "case when sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when a.optype<>1 then 1 else 0 end))*100) else 0 end as 转化率,"
                    + "sum(case when c.resultCode=0 and c.optype=1 then 1 else 0 end) as 同步退订总量,"
                    + "sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end) as 同步成功总量,"
                    + "case when sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))*100) else 0 end as 同步率,"
                    + "sum(case when c.resultCode=0 and c.optype=0 then c.fee else 0 end) as 同步有效信息费,"
                    + "sum(case when c.result=1 and c.optype=0 then 1 else 0 end) as 回掉失败 "
                    + "from public_notify_2017 as a WITH(NOLOCK) "
                    + "left join public_sync_2017 as c on c.infoid=a.infoid "
                    + "where a.infoid>0" + param
                    + " group by SUBSTRING(a.area,0,CHARINDEX(' ',a.area))"
                    + ") "
                    + "select a.name as 省份,x4.上行总量,x4.验证码总量,x4.通知订购总量,x4.独立用户,x4.成功订购总量,x4.退订总量,x4.hours,x4.有效信息费,x4.转化率,x4.同步退订总量,x4.同步成功总量,x4.同步率,x4.同步有效信息费,x4.回掉失败 from areaInfo as a left join x4 on x4.省份=a.name where a.parent_id=0 order by a.sort";
                   */

                   string sqla = "select (case when SUBSTRING(area,0,CHARINDEX(' ',area)) is null then '[无号码]' when SUBSTRING(area,0,CHARINDEX(' ',area))='' then '[未识别]' else SUBSTRING(area,0,CHARINDEX(' ',area)) end) as 省份,sum(case when code is null or code='' then 1 else 0 end) as 上行总量, COUNT(DISTINCT(case when buyid=2 and len(IMSI)>0 and len(code)>0 then IMSI when buyid=2 and len(mobile)>0 and len(code)>0 then mobile end))+COUNT(case when buyid=1 and len(IMSI)>0 and len(code)>0 then IMSI when buyid=1 and len(mobile)>0 and len(code)>0 then mobile end) as 独立验证码, COUNT(case when len(code)>0 and len(IMSI)>0 then IMSI when len(code)>0 and len(mobile)>0 then mobile end) as 验证码总量 from public_order_2017 WITH(NOLOCK) where infoid>0 " + param.Replace("a.", "") + " group by SUBSTRING(area,0,CHARINDEX(' ',area))";
                   DataTable dt1 = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sqla).Tables[0];


                   sql1 = "select count(a.infoid) from public_notify_2017 as a WITH(NOLOCK) where (select count(e.infoid) from public_notify_2017 as e WITH(NOLOCK) where e.resultCode=0 and e.buyID=2 and e.optype=1 and SUBSTRING(e.area,0,CHARINDEX(' ',e.area))='{0}' and e.mobile=a.mobile and CONVERT(varchar(30),e.datatime,120)<CONVERT(varchar(30),DATEADD(hh,+72,a.datatime),120)" + param1.Replace("a.", "e.") + ")>0 and a.resultCode=0 and a.buyID=2 and a.optype=0 and SUBSTRING(a.area,0,CHARINDEX(' ',a.area))='{0}'" + param;
                  dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

                  if (dt1.Rows.Count > 0)
                  {
                      for (int i = 0; i < dt1.Rows.Count; i++)
                      {

                          DataRow[] temp1 = dt.Select("省份='" + dt1.Rows[i]["省份"].ToString() + "'");

                          if (temp1.Length > 0)
                          {
                              temp1[0]["上行总量"] = Convert.ToInt32(dt1.Rows[i]["上行总量"]);
                              temp1[0]["验证码总量"] = Convert.ToInt32(dt1.Rows[i]["验证码总量"]);
                              temp1[0]["独立验证码"] = Convert.ToInt32(dt1.Rows[i]["独立验证码"]);

                              double code = Convert.ToDouble(dt1.Rows[i]["独立验证码"]);
                              double order = Convert.ToDouble(dt1.Rows[i]["上行总量"]);
                              if (!string.IsNullOrEmpty(temp1[0]["通知订购总量"].ToString()))
                              {
                                  double notify = Convert.ToDouble(temp1[0]["通知订购总量"].ToString());
                                  double result = Convert.ToDouble(temp1[0]["成功订购总量"].ToString());

                                  if (code > 0)
                                  {
                                      double num1 = result / code;
                                      temp1[0]["转化率"] = Math.Round(num1 * 100, 2);
                                  }
                              }
                          }
                          else
                          {
                              DataRow dRow = dt.NewRow();
                              dRow["省份"] = dt1.Rows[i]["省份"].ToString();
                              dRow["上行总量"] = Convert.ToInt32(dt1.Rows[i]["上行总量"]);
                              dRow["验证码总量"] = Convert.ToInt32(dt1.Rows[i]["验证码总量"]);
                              dRow["独立验证码"] = Convert.ToInt32(dt1.Rows[i]["独立验证码"]);
                             
                              dt.Rows.Add(dRow);
                          }

                      }
                  }
                  
                  foreach (DataRow dr in dt.Rows)
                  {
                      dr["hours"] = get72Hours(string.Format(sql1, dr["省份"].ToString()));
                  }
            
            }
            else
            {
                sql =
                  "select d.name as 公司名称,"
                  + "a.companyID,a.productID,a.conduitID,e.names as 通道,"
                  + "(select name from product where infoid=a.productID) as 产品名称,"
                  + "ISNULL(a.fee,0) as 资费,"
                  + "0 as 上行总量,"
                  + "0 as 验证码总量,"
                  + "0 as 独立验证码,"
                  + "0 as hours,"
                  + "sum(case when a.optype<>1 then 1 else 0 end) as 通知订购总量,"
                  + "COUNT(DISTINCT(case when a.optype<>1 then a.mobile end)) as 独立用户,"
                  + "sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end) as 成功订购总量,"
                  + "sum(case when a.resultCode=0 and a.optype=1 then 1 else 0 end) as 退订总量,"
                  + "sum(case when a.resultCode=0 and a.optype=0 then a.fee else 0 end) as 有效信息费,"
                  + "case when sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when a.optype<>1 then 1 else 0 end))*100) else 0 end as 转化率,"
                  + "sum(case when c.resultCode=0 and c.optype=1 then 1 else 0 end) as 同步退订总量,"
                  + "sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end) as 同步成功总量,"
                  + "case when sum(case when c.resultCode=0 and a.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))*100) else 0 end as 同步率,"
                  + "sum(case when c.resultCode=0 and c.result=0 and c.optype=0 then c.fee else 0 end) as 同步有效信息费,"
                  + "sum(case when c.result=1 and c.optype=0 then 1 else 0 end) as 回掉失败 "
                  + "from public_sync_2017 as c WITH(NOLOCK) "
                  + "right join public_notify_2017 as a WITH(NOLOCK) on c.infoid=a.infoid "
                  + "left join conduit as e on e.infoid=a.conduitid "
                  + "left join company as d on d.infoid=a.companyid "
              
                  + "where d.infoid>0" + param
                  + " group by a.companyID,a.conduitID,a.fee,d.name,e.names,a.productID";
                  
                  
                /*sql = "select * from(select (select top 1 name from company where infoid=a.companyid) as 公司名称,"
                 + "a.companyID,a.productID,a.conduitID,"
                 + "(select top 1 names from conduit where infoid=a.conduitid) as 通道,"
                 + "(select top 1 name from product where infoid=a.productID) as 产品名称,"
                 + "ISNULL(a.fee,0) as 资费,"
                 + "0 as 上行总量,"
                 + "0 as 验证码总量,"
                 + "0 as 独立验证码,"
                 + "0 as hours,"
                 + "sum(case when a.optype<>1 then 1 else 0 end) as 通知订购总量,"
                 + "COUNT(DISTINCT(case when a.optype<>1 then (case when len(a.mobile)>0 then a.mobile else a.imsi end) end)) as 独立用户,"
                 + "sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end) as 成功订购总量,"
                 + "sum(case when a.resultCode=0 and a.optype=1 then 1 else 0 end) as 退订总量,"
                 + "sum(case when a.resultCode=0 and a.optype=0 then a.fee else 0 end) as 有效信息费,"
                 + "case when sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when a.optype<>1 then 1 else 0 end))*100) else 0 end as 转化率,"
                 + "sum(case when c.resultCode=0 and c.optype=1 then 1 else 0 end) as 同步退订总量,"
                 + "sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end) as 同步成功总量,"
                 + "case when sum(case when c.resultCode=0 and a.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))*100) else 0 end as 同步率,"
                 + "sum(case when c.resultCode=0 and c.result=0 and c.optype=0 then c.fee else 0 end) as 同步有效信息费,"
                 + "sum(case when c.result=1 and c.optype=0 then 1 else 0 end) as 回掉失败 "
                 + "from  as c WITH(NOLOCK) "
                 + "right join public_notify_2017 as a WITH(NOLOCK) on c.infoid=a.infoid "
                 + "where a.infoid>0" + param
                 + "group by a.conduitID,a.fee,a.productID,a.companyID) as tab order by 公司名称";
                
                  /*string sqla=" WITH x1 AS ("
                  + "select b.name,a.companyID,a.conduitID,a.fee,COUNT(a.userOrder) as 提交总量,0 as 验证码总量,0 as code from public_order_2017 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyID where a.infoid>0" + param + " group by a.companyID,a.conduitID,a.fee,b.name"
                  +"),x2 AS ("
                  + "select b.name,a.companyID,a.conduitID,a.fee,0 as 提交总量,COUNT(DISTINCT(a.userOrder)) as 验证码总量,COUNT(a.userOrder) as code from public_order_2017 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyID where len(a.code)>0" + param + " group by a.companyID,a.conduitID,a.fee,b.name"
                  +") "
                  + "Select name,names,companyID,conduitID,fee,sum(验证码总量) as 验证码总量,sum(提交总量)-sum(code) as 上行总量 from" 
                  +"("
                  + "Select b.name,c.names,x1.companyID,x1.conduitID,x1.fee,x1.验证码总量,x1.提交总量,x1.code from x1 left join company as b on x1.companyID=b.infoid left join conduit as c on x1.conduitid=c.infoid "
	              +"Union "
                  + "Select b.name,c.names,x2.companyID,x2.conduitID,x2.fee,x2.验证码总量,x2.提交总量,x2.code from x2 left join company as b on x2.companyID=b.infoid left join conduit as c on x2.conduitid=c.infoid "
                  + ") as a group by companyID,conduitID,fee,name,names";*/

                

                  
                  /*+ "select x0.companyID as infoid,x0.公司名称,x0.通道,x0.资费,(x1.提交总量-x2.code) as 上行总量,x2.验证码总量,x0.通知订购总量,x0.独立用户,x0.成功订购总量,x0.退订总量,0 as hours,x0.有效信息费,"
                   + "(case when x2.验证码总量>0 then "
                    + "Convert(decimal(18,2),Convert(decimal(18,2),x0.成功订购总量)/Convert(decimal(18,2),(x2.验证码总量+(x0.通知订购总量-x0.成功订购总量)))) "
                    + "WHEN x0.成功订购总量>0 then "
                    + "Convert(decimal(18,2),Convert(decimal(18,2),x0.成功订购总量)/Convert(decimal(18,2),x0.通知订购总量)) end)*100 as 转化率,"
                  +"x0.同步退订总量,x0.同步成功总量,x0.同步率,x0.同步有效信息费,x0.回掉失败 from x0 "
	              +"left join x1 on x0.companyID=x1.companyID and x0.conduitID=x1.conduitID and x0.资费=x1.fee "
	              +"left join x2 on x0.companyID=x2.companyID and x0.conduitID=x2.conduitID and x0.资费=x2.fee "
	              +"order by x0.companyID";*/

                if (string.IsNullOrEmpty(param2))
                    sql1 = "select count(a.infoid) from public_notify_2017 as a where (select count(e.infoid) from public_notify_2017 as e where e.resultCode=0 and e.buyID=2 and e.optype=1 and e.mobile=a.mobile and CONVERT(varchar(30),e.datatime,120)<CONVERT(varchar(30),DATEADD(hh,+72,a.datatime),120) and e.fee={0} and e.companyID={1})>0 and a.resultCode=0 and a.buyID=2 and a.optype=0 and a.fee={0} and a.companyID={1}";
                else
                    sql1 = "select count(a.infoid) from public_notify_2017 as a where (select count(e.infoid) from public_notify_2017 as e where e.resultCode=0 and e.buyID=2 and e.optype=1 and e.mobile=a.mobile and CONVERT(varchar(30),e.datatime,120)<CONVERT(varchar(30),DATEADD(hh,+72,a.datatime),120) and e.fee={0} and e.companyID={1}" + param2.Replace("a.", "e.") + ")>0 and a.resultCode=0 and a.buyID=2 and a.optype=0 and a.fee={0} and a.companyID={1}" + param2;


                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];


                string sqla = "select (select top 1 names from conduit where infoid=a.conduitid) as names,(select top 1 name from company where infoid=a.companyid) as name,a.companyID,a.conduitID,a.productID,a.fee,(select name from product where infoid=a.productID) as 产品名称,sum(case when a.code is null or a.code='' then 1 else 0 end) as 上行总量, COUNT(DISTINCT(case when a.buyid=2 and len(a.IMSI)>0 and len(a.code)>0 then a.IMSI when a.buyid=2 and len(a.mobile)>0 and len(a.code)>0 then a.mobile end))+COUNT(case when a.buyid=1 and len(a.IMSI)>0 and len(a.code)>0 then a.IMSI when a.buyid=1 and len(a.mobile)>0 and len(a.code)>0 then a.mobile end) as 独立验证码, COUNT(case when len(a.code)>0 and len(a.IMSI)>0 then a.IMSI when len(a.code)>0 and len(a.mobile)>0 then a.mobile end) as 验证码总量 from public_order_2017 as a WITH(NOLOCK) where a.infoid>0" + param + " group by a.companyID,a.conduitID,a.productID,a.fee";
                DataTable dt1 = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sqla).Tables[0]; 

                if (dt1.Rows.Count > 0)
                {
                    for (int i = 0; i < dt1.Rows.Count; i++)
                    {
                        DataRow[] temp1 = dt.Select("companyID=" + dt1.Rows[i]["companyID"].ToString() + " and conduitID=" + dt1.Rows[i]["conduitID"].ToString() + " and productID=" + dt1.Rows[i]["productID"].ToString() + " and 资费=" + dt1.Rows[i]["fee"].ToString());


                        if (temp1.Length > 0)
                        {
                            
                            double code = Convert.ToDouble(dt1.Rows[i]["独立验证码"]);
                            double order = Convert.ToDouble(dt1.Rows[i]["上行总量"]);
                            double notify = Convert.ToDouble(temp1[0]["通知订购总量"]);
                            double result = Convert.ToDouble(temp1[0]["成功订购总量"]);
                            temp1[0]["上行总量"] = Convert.ToInt32(dt1.Rows[i]["上行总量"]);
                            temp1[0]["验证码总量"] = Convert.ToInt32(dt1.Rows[i]["验证码总量"]);
                            temp1[0]["独立验证码"] = Convert.ToInt32(dt1.Rows[i]["独立验证码"]);
                            if (code > 0)
                            {
                                double num1 = result / code ;
                                temp1[0]["转化率"]=Math.Round(num1 * 100, 2);
                                
                            }
                            
                        }
                        else
                        {
                            
                            DataRow dRow = dt.NewRow();
                            dRow["公司名称"] = dt1.Rows[i]["name"].ToString();
                            dRow["通道"] = dt1.Rows[i]["names"].ToString();
                            dRow["产品名称"] =dt1.Rows[i]["产品名称"].ToString();
                            dRow["上行总量"] = Convert.ToInt32(dt1.Rows[i]["上行总量"]);
                            dRow["验证码总量"] = Convert.ToInt32(dt1.Rows[i]["验证码总量"]);
                            dRow["独立验证码"] = Convert.ToInt32(dt1.Rows[i]["独立验证码"]);
                            dRow["资费"] = Convert.ToInt32(dt1.Rows[i]["fee"]);
                            
                            dt.Rows.Add(dRow);
                        }
                    }
                }
                dt.DefaultView.Sort = "公司名称 ASC";
                dt = dt.DefaultView.ToTable();

                /*foreach (DataRow dr in dt.Rows)
                {
                    dr["hours"] = get72Hours(string.Format(sql1, dr["资费"].ToString(), dr["infoid"].ToString()));
                }*/
            
            }

            //dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            /*foreach (DataRow dr in dt.Rows)
            {

            }*/

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";


            return info;
        }


        static int get72Hours(string sql)
        {
            int info = 0;
             
            object obj=SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
            if (null != obj)
               info = Convert.ToInt32(obj);

            return info;
            
        }


        /// <summary>
        /// 获取通道列表
        /// </summary>
        /// <returns></returns>
        public static string getConduitList()
        {
            string info = "{data:[]}";
            string sql = "SELECT b.infoid,a.names+'--'+ b.names as names FROM conduit as a left join conduit as b on b.pid=a.infoid where a.pid=0 and a.flag=0 order by a.names";
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {
                DataRow dr = dt.NewRow();
                dr["infoid"] = -1;
                dr["names"] = "全部通道";
                dt.Rows.Add(dr);
                dt.AcceptChanges();

                /*DataTable dcopy = dt.Copy();
                DataView dv = dt.DefaultView;
                dv.Sort = "infoid asc";
                dcopy = dv.ToTable();*/

                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";
            }

            return info;
        }


        /// <summary>
        /// 获取产品列表
        /// </summary>
        /// <returns></returns>
        public static string getProductList(int id)
        {
            string info = "{data:[]}";
            string sql = "SELECT infoid,name FROM product order by conduitid";

            if(id>0)
              sql = "SELECT infoid,name FROM product where conduitid=" + id + " order by conduitid";
            
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {
                DataRow dr = dt.NewRow();
                dr["infoid"] = -1;
                dr["name"] = "全部产品";
                dt.Rows.Add(dr);

                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";
            }

            return info;
        }


        public static string setNoteValue(string data)
        {
            string info = "设置完成！";
            

            SqlConnection conn = new SqlConnection(SqlHelper.ConnectionStringLocalTransaction);
            conn.Open();
            using (SqlTransaction trans = conn.BeginTransaction())
            {
                try
                {
                    string[] list = data.Split(',');
                    string temp = string.Empty;
                    for (int i = 0; i < list.Length; i++)
                    {
                        temp += ",'" + list[i] + "'";
                    }

                    string sql1 = "delete blacklist where mobile in(" + temp.Substring(1)+ ")";
                    SqlHelper.ExecuteNonQuery(trans, CommandType.Text, sql1);

                    for (int i = 0; i < list.Length; i++)
                    {

                        string sql = "INSERT INTO [blacklist]([mobile],[rulesid],[typeID]) values('" + list[i] + "',3,2)";
                        SqlHelper.ExecuteNonQuery(trans, CommandType.Text, sql);

                    }
                   

                    trans.Commit();

                    

                }
                catch (Exception e)
                {
                    info = "设置失败！";
                    trans.Rollback();
                    LogHelper.WriteLog(typeof(dataManage), "============>setRefund异常[" + e.ToString() + "]");

                }
            }
            conn.Close();


            return info;
        }

        
     /*public static string ManualSyncData(string idlist)
     {
         JObject ainfo = new JObject();
         JArray datainfo = new JArray();
         if (string.IsNullOrEmpty(idlist))
             return "";
         
             foreach (JObject item in datainfo)
             {

               //  DataTable dt = getConfigInfo(Convert.ToInt32(item["conduitid"]),4);

                 if (dt.Rows.Count == 1)
                 {
                     string jdata = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
                     JObject json = JObject.Parse(jdata);

                     string sqla = "select * from " + json[" tablename"].ToString() + " where infoid=" + item["infoid"].ToString() + " and conduitid=" + item["conduitid"].ToString();
                     DataTable dta = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sqla).Tables[0];
                     string cp = getActionInfo(Convert.ToInt32(dta.Rows[0]["companyid"]), Convert.ToInt32(dta.Rows[0]["fee"]), Convert.ToInt32(dta.Rows[0]["conduitid"]));
                     if (!string.IsNullOrEmpty(cp))
                     {
                         ainfo = JObject.Parse(cp);

                         if (string.IsNullOrEmpty(ainfo["syncUrl"].ToString()))
                             return "";


                         //获取要插入同步表的数据字段
                         DataTable field = getField(dt.Rows[0]["infoid"].ToString(), dt.Rows[0]["conduitid"].ToString());


                         //获取同步接口字段
                         DataTable syncfield = getSyncInterfaceField(Convert.ToInt32(dt.Rows[0]["infoid"]));
                         if (syncfield.Rows.Count == 0)
                         {

                             return "";
                         }

                         //获取同步接口数据
                         bool status = getSyncInterfaceData(dta, syncfield);
                         if (status == false)
                         {

                             return "";
                         }

                         string param = setFormatInterfaceData(Convert.ToInt32(dt.Rows[0]["syncFormat"]), syncfield, dt.Rows[0]["syncCode"].ToString());
                         if (string.IsNullOrEmpty(param))
                         {
                            
                             return "";
                         }



                         int olid = 1; //AddSyncData(field, dt.Rows[0]["tablename"].ToString(), dt.Rows[0]["tablename"].ToString(), correlator[0]["value"].ToString(), "syncpack", ainfo["syncUrl"].ToString() + "?" + param);
                         if (olid > 0)
                         {
                             //req.Response.Write(streamingno[0]["StreamingNo"].ToString() + "\r\n" + uptab);
                             //string flag = Utils.GetResponseData(param, ainfo["syncUrl"].ToString());
                             string flag = Utils.GetService(ainfo["syncMethod"].ToString(), ainfo["syncUrl"].ToString(), param);
                         }
                     }

                 }//end if

             }
       
             return "";

     }*/



     public static string getActionInfo(int companyid, int fee, int conduitid)
     {
         string sql = null;
         string info = null;
         try
         {
             sql = "select top 1 a.productid,a.ordered as message,b.address as senderAddress,a.buyid,a.infoid as actionid,REPLACE(REPLACE(REPLACE(REPLACE(convert(varchar(50), sysdatetime()),'-',''),':',''),' ',''),'.','') as transactionId"
                 + " from action as a left join product as b on b.infoid=a.productid"
                 + " where companyid=" + companyid + " and price=" + fee + " and a.conduitid=" + conduitid + " and syncstart=1";
             DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
             if (dt.Rows.Count > 0)
                 info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(dataManage), sql + "============>getActionInfo(int companyid, int fee, int conduitid)获取数据异常[" + e.ToString() + "]");
             return info;
         }

         return info;
     }


     public static string getActionList(int companyid,int productid)
     {
         string sql = null;
         string info = null;
         try
         {
             sql = "select *,(select name product where infoid=a.productid) as productname from action as a where a.companyid=" + companyid + " and productid=" + productid;    
             DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
             if (dt.Rows.Count > 0)
                 info = JsonConvert.SerializeObject(dt, new DataTableConverter());
         }
         catch (Exception e)
         {
            
             return info;
         }

         return info;
     }


     public static DataTable getField(string configid, string conduitid)
     {

         string sql = null;
         DataTable dt = new DataTable();
         try
         {
             sql = "select * from public_field where configid=" + configid + " and conduitid=" + conduitid;
             dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(dataManage), sql + "============>getField获取数据异常[" + e.ToString() + "]");
             return dt;
         }

         return dt;
     }



     /// <summary>
     /// 获取同步接口字段
     /// </summary>
     /// <param name="configid"></param>
     /// <returns></returns>
     public static DataTable getSyncInterfaceField(int configid)
     {
         string sql = null;
         string field = string.Empty;
         DataTable dt = new DataTable();
         try
         {
             sql = "select * from public_interface where configid=" + configid;
             dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(dataManage), sql + "============>getSyncInterfaceField获取数据异常[" + e.ToString() + "]");
             return dt;
         }

         return dt;
     }


     /// <summary>
     /// 获同步取接口参数数据
     /// </summary>
     /// <param name="data"></param>
     /// <param name="field"></param>
     /// <returns></returns>
     public static bool getSyncInterfaceData(DataTable data, DataTable field)
     {
         string sql = null;
         bool falg = true;
         try
         {
             field.Columns.Add("value", typeof(string));
             data.CaseSensitive = false;
             foreach (DataRow dr in field.Rows)
             {
                 if (dr["isRequest"].ToString() == "1")
                 {
                     DataRow[] value = data.Select("paramname='" + dr["inField"].ToString() + "'");
                     if (value.Length > 0)
                         dr["value"] = value[0]["value"].ToString();
                     else
                         dr["value"] = string.Empty;
                 }
                 else
                 {
                     DataRow[] value = data.Select("fieldname='" + dr["inField"].ToString() + "'");
                     if (value.Length > 0)
                         dr["value"] = value[0]["value"].ToString();
                     else
                         dr["value"] = dr["inField"].ToString();
                 }
             }
         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(dataManage), sql + "============>getSyncInterfaceData获取数据异常[" + e.ToString() + "]");
             return false;
         }

         return falg;
     }



     /// <summary>
     /// 设置对外输出接口格式
     /// </summary>
     /// <param name="fromatid">格式id</param>
     /// <param name="data">数据</param>
     /// <param name="fromatCode">格式编码</param>
     /// <returns></returns>
     public static string setFormatInterfaceData(int fromatid, DataTable data, string fromatCode)
     {
         string param = string.Empty;
         data.DefaultView.Sort = "sort ASC";
         data = data.DefaultView.ToTable();
         data.Columns.Remove("infoid");
         data.Columns.Remove("sort");
         if (fromatid == 2)
         {
             foreach (DataRow dr in data.Rows)
                 param += "&" + dr["outField"].ToString() + "=" + dr["value"].ToString();
             param = param.Substring(1);
         }
         else if (fromatid == 1 || fromatid == 0)
         {
             for (int i = 0; i < data.Rows.Count; i++)

                 param = FindJsonNodex(param, data.Rows[i]["outField"].ToString(), data.Rows[i]["value"].ToString());
         }

         return param;
     }



     public static string FindJsonNodex(string json, string node, string value)
     {
         string data = string.Empty;
         int spoint = json.IndexOf(node);
         if (spoint < 0)
             return null;
         string str = json.Substring(spoint);
         int epoint = str.IndexOf(",");
         if (epoint == -1)
             data = json.Substring(spoint, (str.Length - 1));
         else
             data = json.Substring(spoint, epoint);
         JObject redata = JObject.Parse("{" + data + "}");
         redata["node"] = value;
         return json.Replace(data, redata["node"].ToString()).Trim();
     }


    public static int AddSyncData(string field, string value)
     {
        
         int flag = 0;
         string sql = null;
         try
         {
            sql = "INSERT INTO public_sync_2017(" + field + ") values("+value+")";
            flag = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(dataManage), sql + "============>AddSyncData插入数据异常[" + e.ToString() + "]");
             return flag;
         }
         return flag;
     }


     /// <summary>
     /// 获取指定业务步骤配置信息
     /// </summary>
     /// <param name="condutiid"></param>
     /// <param name="step"></param>
     /// <returns></returns>
     public static DataTable getConfigField(int conduitid,int configid)
     {
         string sql = null;
        
         DataTable dt = new DataTable();
         try
         {
             sql = string.Format("select fieldname from public_field where configid={0} and conduitid={1}",configid,conduitid);
             dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            
         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(dataManage), sql + "============>getConfigInfo(int conduitid,int configid)获取数据异常[" + e.ToString() + "]");
             return dt;
         }

         return dt;
      }


     /*public static string getConfigField(int conduitid, int step)
     {
         string sql = null;
         string field = null;
         DataTable dt = new DataTable();
         try
         {
             sql = "select top 1 (select ','+fieldname from public_field as b where b.conduitid=a.conduitid and b.configid=a.infoid For XML Path('')) as fieldname from public_Config as a where a.conduitid=" + conduitid + " and step=" + step;
             dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
             if(dt.Rows.Count>0)
                 field=dt.Rows[0][0].ToString();
         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(dataManage), sql + "============>getConfigField(int conduitid, int step)获取数据异常[" + e.ToString() + "]");
             return field;
         }

         return field;
     }*/


     public static DataTable getInterfaceField(int configid)
     {
         string sql = null;
         DataTable dt = new DataTable();
         try
         {
             sql = "select inField,outField from public_interface where configid=" + configid + " order by sort";
             dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(dataManage), sql + "============>getInterfaceField(int configid)获取数据异常[" + e.ToString() + "]");
             return dt;
         }

         return dt;
     }

     /// <summary>
     /// 获取需要同步的数据
     /// </summary>
     /// <param name="idlist">数据id序列</param>
     /// <param name="tabname">查询表</param>
     /// <returns></returns>
     public static DataTable getSynData(string idlist)
     {
         DataTable dt = new DataTable();
         string listid="";
         string value = "";
         string param="";
         try
         {
             if (string.IsNullOrEmpty(idlist))
                 return dt;

             string sql = "select a.*,b.configid,b.syncFlag,b.syncUrl,b.syncStart,b.syncMethod,flag=0,(select top 1 case when infoid>0 then 1 else 0 end from public_sync_2017 WITH(NOLOCK) where infoid=a.infoid) as notify from public_notify_2017 as a WITH(NOLOCK) left join action as b on b.conduitid=a.conduitid and b.companyid=a.companyid and b.productid=a.productid  where a.infoid in (" + idlist + ")";
             dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
             if (dt.Rows.Count > 0)
             {
                 string items = string.Empty;
                 dt.CaseSensitive = false;
                 DataRow[] stopid = dt.Select("(syncStart=0 or syncFlag=0) and (syncUrl is null or len(syncUrl)=0) and notify=1");
                 foreach (DataRow dr in stopid)
                     dr["flag"] = 1;

                 DataRow[] startid = dt.Select("flag=0");
                 
                 foreach (DataRow dr in startid)
                 {
                     DataTable field=getInterfaceField(Convert.ToInt32(dr["configid"]));

                     foreach (DataRow itemid in field.Rows)
                       {
                           string names=itemid["fieldname"].ToString();
                           DataRow[] inField = dt.Select("inField='"+names+"'");
                           if(inField.Length>0)
                               param += "&"+inField[0]["outField"].ToString()+"="+ dr[names].ToString();
                           if(null!=dr[names])
                           {
                             listid+=","+names;
                             value+=","+dr[names].ToString();
                           }
                       }
                     

                         int flag = 0;//AddSyncData("syncpack" + listid, param.Substring(1) + value);
                         //if (flag == 0)
                             //dr["result"] = 1;
                     
                       listid="";
                       value = "";
                       param="";
                 }
             }
         }
         catch (Exception e)
         {
             return dt;
         }

         return dt;
     }


     static int updateSyncData(string idlist)
     {
         int old = 0;
         try
         {
             string sql = "update public_sync_2017 WITH(NOLOCK) set result=0 where infoid in(" + idlist + ") and result=1";
             old = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);
         }
         catch (Exception e)
         {
             LogHelper.WriteLog(typeof(dataManage), "============>updateSyncData更新数据异常[" + e.ToString() + "]");
             return 0;
         }
         return old;
     }

        static bool BulkToDB(DataTable dt)
        {
            bool flag=true;
            using (SqlConnection connection = new SqlConnection(SqlHelper.ConnectionStringLocalTransaction))
            {
                try
                {
                        SqlBulkCopy bulkCopy = new SqlBulkCopy(connection);
                        bulkCopy.DestinationTableName = "public_sync_2017";
                        bulkCopy.BatchSize = dt.Rows.Count;
                        connection.Open();
             
                        if (dt != null && dt.Rows.Count != 0)
                        {
                            bulkCopy.WriteToServer(dt);
                            bulkCopy.Close();
                        }
                }
                catch (Exception e)
                {
                   LogHelper.WriteLog(typeof(dataManage),"============>BulkToDB插入数据异常[" + e.ToString() + "]");
                   return false;
                }
                finally
                {
                    connection.Close();
                }
            }

            return flag;
        }


        /// <summary>
        /// 导出统计数据
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public static string exportCode(string field,string data)
        {
            string info = null;
            string names = DateTime.Now.ToString("yyyyMMddhhmmss") + ".csv";
            string path = AppDomain.CurrentDomain.SetupInformation.ApplicationBase + ConfigurationManager.AppSettings["cvspath"];

            string param = "";
            string param2 = "";
            string sql = null;
            string idlist = "";
            string[] items = field.Replace("[", "").Replace("]", "").Replace("\"","").Split(',');
            for (int i = 0; i < items.Length; i++)
                idlist += ",a." + items[i];
                if (!string.IsNullOrEmpty(field))
                {
                    JObject datajson = JObject.Parse(data);

                    string stime = datajson["stime"].ToString();
                    string etime = datajson["etime"].ToString();

                    if (stime.Length < 5)
                        datajson["stime"] = "0" + stime;
                    if (etime.Length < 5)
                        datajson["etime"] = "0" + etime;


                    if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                    {

                        string eday = datajson["edate"].ToString();
                        string[] temp = eday.Split('-');
                        if (temp[1].Length < 2)
                            temp[1] = "0" + temp[1];
                        if (temp[2].Length < 2)
                            temp[2] = "0" + temp[2];
                        datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];
                        param += "a.datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"] + "' and  DATEADD(MINUTE,+1,'" + datajson["edate"].ToString() + " " + datajson["etime"] + "')";
                        param2 = param;
                    }

                    if (null!=datajson["conduit"])
                    {
                        if (Convert.ToInt32(datajson["conduit"]) > 0)
                        {
                            param += " and a.conduitID=" + datajson["conduit"].ToString();
                            param2 = param;
                        }
                    }

                    if (!string.IsNullOrEmpty(datajson["company"].ToString()))
                    {
                        if (Convert.ToInt32(datajson["company"]) > 0)
                        {
                            param += " and a.companyID=" + datajson["company"].ToString();
                            param2 = param;
                        }
                    }

                    if (!string.IsNullOrEmpty(datajson["product"].ToString()))
                    {
                        if (Convert.ToInt32(datajson["product"]) > 0)
                        {
                            param += " and a.productID=" + datajson["product"].ToString();
                            param2 = param;
                        }
                    }
                    if (!string.IsNullOrEmpty(datajson["verifycode"].ToString()))
                    {
                        string codeflag = string.Empty;
                        if (datajson["verifycode"].ToString().IndexOf("[") > -1)
                        {
                            JArray lsa = JArray.Parse(datajson["verifycode"].ToString());

                            for (int i = 0; i < lsa.Count; i++)
                                codeflag += "," + lsa[i].ToString();
                            param += " and a.codeflag in(" + codeflag.Substring(1) + ")";
                        }
                        else
                            param += " and a.codeflag=" + Convert.ToInt32(datajson["verifycode"]);
                    }

                    if (null != datajson["area"] && !string.IsNullOrEmpty(datajson["area"].ToString()))
                    {
                        string area = "";
                        if (datajson["area"].ToString().IndexOf("[") != -1)
                        {
                            JArray lsa = JArray.Parse(datajson["area"].ToString());

                            for (int i = 0; i < lsa.Count; i++)
                                area += "or a.area like '" + lsa[i].ToString() + "%' ";

                            param += " and (" + area.Substring(2) + ") ";
                            param2 = param;
                        }
                        else
                        {
                            param += "and a.area like '" + datajson["area"].ToString() + "%' ";
                            param2 = param;
                        }
                    }

                }
                sql = string.Format("select {0} from public_order_2017 as a WITH(NOLOCK) where {1} and not exists(select userOrder from public_notify_2017 where a.userOrder=userOrder and resultCode=0 and {2}) order by a.datatime desc", idlist.Substring(1), param, param2.Replace("a.", ""));
            /*sql = "SELECT a.mobile as 卡号,a.total as 退费金额,(select count(b.infoid) from [refund] as b where b.mobile=a.mobile) as 扣费次数,"
                + "公司=STUFF((SELECT ','+b.name FROM [refund] t left join company as b on b.infoid=t.companyID WHERE t.mobile=a.mobile group by b.name order by b.name FOR XML PATH('')), 1, 1, '')"
                + "FROM [refund] as a where a.infoid>0 " + param + " group by a.mobile,a.total";*/

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {
                bool flag = exportRefund(dt, path + names);
                if (flag)
                    info = names;
            }
            return info;
        }


        public static string exportCode(string field, string data,string node)
        {
            string info = null;
            string names = DateTime.Now.ToString("yyyyMMddhhmmss") + ".csv";
            string path = AppDomain.CurrentDomain.SetupInformation.ApplicationBase + ConfigurationManager.AppSettings["cvspath"];
            string tabname = "public_order_2017";
            string param = "";
            string sql = null;
            string idlist = "";
            string[] items = field.Replace("[", "").Replace("]", "").Replace("\"", "").Split(',');
            for (int i = 0; i < items.Length; i++)
                idlist += ",a." + items[i];
            if (!string.IsNullOrEmpty(field))
            {
                JObject datajson = JObject.Parse(data);

                string stime = datajson["stime"].ToString();
                string etime = datajson["etime"].ToString();

                if (stime.Length < 5)
                    datajson["stime"] = "0" + stime;
                if (etime.Length < 5)
                    datajson["etime"] = "0" + etime;


                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {

                    string eday = datajson["edate"].ToString();
                    string[] temp = eday.Split('-');
                    if (temp[1].Length < 2)
                        temp[1] = "0" + temp[1];
                    if (temp[2].Length < 2)
                        temp[2] = "0" + temp[2];
                    datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];
                    param += "a.datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"] + "' and  DATEADD(MINUTE,+1,'" + datajson["edate"].ToString() + " " + datajson["etime"] + "')";
                  
                }

                if (null != datajson["flag"])
                {
                    if (datajson["flag"].ToString().IndexOf("[") == -1)
                        param += " and a.resultCode=" + datajson["flag"].ToString();
                }

                if (null != datajson["verifycode"])
                {
                    if (datajson["verifycode"].ToString().IndexOf("[") == -1)
                        param += " and a.codeflag=" + datajson["verifycode"].ToString();
                }
                else
                    tabname = "public_notify_2017";

                if (null != datajson["optype"])
                {
                    if (datajson["optype"].ToString().IndexOf("[") == -1)
                        param += " and a.optype=" + datajson["optype"].ToString();
                }

                if (!string.IsNullOrEmpty(node))
                {
                    JObject json = JObject.Parse(node);
                    if (json["level"].ToString() == "0")
                        param += " and a.conduitID in (" + json["list"].ToString() + ")";
                    else if (json["level"].ToString() == "1")
                        param += " and a.productID=" + json["infoid"].ToString();
                    else if (json["level"].ToString() == "2")
                        param += " and a.productID=" + json["pid"].ToString() + " and a.companyID=" + json["infoid"].ToString();
                }

                if (null != datajson["area"] && !string.IsNullOrEmpty(datajson["area"].ToString()))
                {
                    string area = "";
                    if (datajson["area"].ToString().IndexOf("[") != -1)
                    {
                        JArray lsa = JArray.Parse(datajson["area"].ToString());

                        for (int i = 0; i < lsa.Count; i++)
                            area += "or a.area like '" + lsa[i].ToString() + "%' ";

                        param += " and (" + area.Substring(2) + ") ";
                    
                    }
                    else
                      param += "and a.area like '" + datajson["area"].ToString() + "%' ";
                }
            }
            sql = string.Format("select {0} from {1} as a WITH(NOLOCK) where {2} order by a.datatime desc", idlist.Substring(1),tabname, param);
           
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {
                bool flag = exportRefund(dt, path + names);
                if (flag)
                    info = names;
            }
            return info;
        }



/*---------------------------------------------2018方法------------------------------------------------------------*/
        public static string getMultiServiceDatax(string search)
        {
            string info = null;
            string sql = null;
            string param = "";
            JObject datajson = JObject.Parse(search);
            string stime = datajson["stime"].ToString();
            string etime = datajson["etime"].ToString();
            DataTable dt = new DataTable();
           
            if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
            {

                string eday = datajson["edate"].ToString();
                string[] temp = eday.Split('-');
                if (temp[1].Length < 2)
                    temp[1] = "0" + temp[1];
                if (temp[2].Length < 2)
                    temp[2] = "0" + temp[2];
                datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];
                param += "a.datatime between '" + datajson["sdate"].ToString() + "' and '" + datajson["edate"].ToString() + "'";
              
            }

            if (!string.IsNullOrEmpty(datajson["conduit"].ToString()))
            {
                if (Convert.ToInt32(datajson["conduit"]) > 0)
                    param += " and a.conduitID=" + datajson["conduit"].ToString();
            }

            if (!string.IsNullOrEmpty(datajson["company"].ToString()))
            {
                if (Convert.ToInt32(datajson["company"]) > 0)
                    param += " and a.companyID=" + datajson["company"].ToString();
            }

            if (!string.IsNullOrEmpty(datajson["product"].ToString()))
            {
                if (Convert.ToInt32(datajson["product"]) > 0)
                    param += " and a.productID=" + datajson["product"].ToString();
            }

            if (Convert.ToInt32(datajson["index"]) == 1)
            {

                sql = "select CONVERT(varchar(10), a.datatime, 120) as 日期,"
                  +"sum(upstream) as 上行总量,"
                  +"sum(verifycode) as 验证码总量,"
                  +"sum(singlecode) as 独立验证码,"
                  +"sum(hours72) as hours,"
                  +"sum(notifyall) as 通知订购总量,"
				  +"sum(singleuser) as 独立用户,"
                  +"sum(orderall) as 成功订购总量,"
                  +"sum(cancelall) as 退订总量,"
                  +"sum(a.amount) as 有效信息费,"
                  + "case when sum(orderall)>0 and sum(singlecode)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(orderall))/Convert(decimal(18,2),sum(singlecode))*100) when sum(upstream)>0 and sum(singlecode)=0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(orderall))/Convert(decimal(18,2),sum(upstream))*100) else 0 end as 转化率,"
                  +"sum(synccancel) as 同步退订总量,"
                  +"sum(syncorders) as 同步成功总量,"
                  +"case when sum(orderall)>0 then Convert(decimal(18,2),sum(syncorders)/Convert(decimal(18,2),sum(orderall))*100) else 0 end as 同步率,"
                  +"sum(syncamount) as 同步信息费,"
                  +"sum(result) as 回掉失败 "
                  +"from public_statistics as a WITH(NOLOCK) "
                  +"left join conduit as e on e.infoid=a.conduitid "
                  +"left join company as d on d.infoid=a.companyid "
                  +"where "+param 
                  +"group by a.datatime order by a.datatime asc";

            }
            else if (Convert.ToInt32(datajson["index"]) == 2)
            {
                sql = "select f.name as 省份,tab.* from(select "
				  +"a.area,"
                  +"sum(upstream) as 上行总量,"
                  +"sum(verifycode) as 验证码总量,"
                  +"sum(singlecode) as 独立验证码,"
                  +"sum(hours72) as hours,"
                  +"sum(notifyall) as 通知订购总量,"
				  +"sum(singleuser) as 独立用户,"
                  +"sum(orderall) as 成功订购总量,"
                  +"sum(cancelall) as 退订总量,"
                  +"sum(a.amount) as 有效信息费,"
                  + "case when sum(orderall)>0 and sum(singlecode)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(orderall))/Convert(decimal(18,2),sum(singlecode))*100) when sum(upstream)>0 and sum(singlecode)=0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(orderall))/Convert(decimal(18,2),sum(upstream))*100) else 0 end as 转化率,"
                  +"sum(synccancel) as 同步退订总量,"
                  +"sum(syncorders) as 同步成功总量,"
                  +"case when sum(orderall)>0 then Convert(decimal(18,2),sum(syncorders)/Convert(decimal(18,2),sum(orderall))*100) else 0 end as 同步率,"
                  +"sum(syncamount) as 同步信息费,"
                  +"sum(result) as 回掉失败 "
                  +"from public_statistics as a WITH(NOLOCK) "
                  +"left join conduit as e on e.infoid=a.conduitid "
                  +"left join company as d on d.infoid=a.companyid "
                  +"where "+param 
                  +" group by a.area) as tab full join areaInfo as f on f.name=tab.area "
				  +"where f.parent_id=0 order by f.sort asc";
            }
            else
            {
                sql =
                  "select d.name as 公司名称,"
                  + "a.companyID,a.productID,a.conduitID,e.names as 通道,"
                  + "(select name from product where infoid=a.productID) as 产品名称,"
                  + "ISNULL(case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end,0) as 资费,"
                  + "sum(upstream) as 上行总量,"
                  + "sum(verifycode) as 验证码总量,"
                  + "sum(singlecode) as 独立验证码,"
                  + "sum(hours72) as hours,"
                  + "sum(notifyall) as 通知订购总量,"
                  + "sum(singleuser) as 独立用户,"
                  + "sum(orderall) as 成功订购总量,"
                  + "sum(cancelall) as 退订总量,"
                  + "sum(a.amount) as 有效信息费,"
                  + "case when sum(orderall)>0 and sum(singlecode)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(orderall))/Convert(decimal(18,2),sum(singlecode))*100) when sum(upstream)>0 and sum(singlecode)=0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(orderall))/Convert(decimal(18,2),sum(upstream))*100) else 0 end as 转化率,"
                  + "sum(synccancel) as 同步退订总量,"
                  + "sum(syncorders) as 同步成功总量,"
                  + "case when sum(orderall)>0 then Convert(decimal(18,2),sum(syncorders)/Convert(decimal(18,2),sum(orderall))*100) else 0 end as 同步率,"
                  + "sum(syncamount) as 同步信息费,"
                  + "sum(result) as 回掉失败 "
                  + "from public_statistics as a WITH(NOLOCK) "
                  + "left join conduit as e on e.infoid=a.conduitid "
                  + "left join company as d on d.infoid=a.companyid "
                  + "where " + param
                  + "group by a.companyID,a.conduitID,a.fee,d.name,e.names,a.productID order by d.name";
            }
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

                if (dt.Rows.Count > 0)
                    info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";
          
            return info;
        }



        public static string getMultiServiceDatax(string search, string node)
        {
            string info = null;
            string sql = null;
            string param = "";
            JObject datajson = JObject.Parse(search);
            //string stime = datajson["stime"].ToString();
            //string etime = datajson["etime"].ToString();
            DataTable dt = new DataTable();

            if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
            {
                string eday = datajson["edate"].ToString();
                string[] temp = eday.Split('-');
                if (temp[1].Length < 2)
                    temp[1] = "0" + temp[1];
                if (temp[2].Length < 2)
                    temp[2] = "0" + temp[2];
                datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];
                param += "a.datatime between '" + datajson["sdate"].ToString() + "' and '" + datajson["edate"].ToString() + "'";
            }

            if (!string.IsNullOrEmpty(node))
            {
                JObject json = JObject.Parse(node);
                if (json["level"].ToString() == "0")
                    param += " and a.conduitID in (" + json["list"].ToString() + ")";
                else if (json["level"].ToString() == "1")
                    param += " and a.productID=" + json["infoid"].ToString();
                else if (json["level"].ToString() == "2")
                    param += " and a.productID=" + json["pid"].ToString() + " and a.companyID=" + json["infoid"].ToString();
            }


            if (Convert.ToInt32(datajson["index"]) == 1)
            {

                sql = "select CONVERT(varchar(10), a.datatime, 120) as 日期,"
                  + "sum(upstream) as 上行总量,"
                  + "sum(verifycode) as 验证码总量,"
                  + "sum(singlecode) as 独立验证码,"
                  + "sum(hours72) as hours,"
                  + "sum(notifyall) as 通知订购总量,"
                  + "sum(singleuser) as 独立用户,"
                  + "sum(orderall) as 成功订购总量,"
                  + "sum(cancelall) as 退订总量,"
                  + "sum(Convert(decimal(18,2),a.amount)) as 信息费,"
                  + "case when sum(orderall)>0 and sum(singlecode)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(orderall))/Convert(decimal(18,2),sum(singlecode))*100) when sum(upstream)>0 and sum(singlecode)=0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(orderall))/Convert(decimal(18,2),sum(upstream))*100) else 0 end as 转化率,"
                  + "sum(synccancel) as 同步退订总量,"
                  + "sum(syncorders) as 同步成功总量,"
                  + "case when sum(orderall)>0 then Convert(decimal(18,2),sum(syncorders)/Convert(decimal(18,2),sum(orderall))*100) else 0 end as 同步率,"
                  + "sum(syncamount) as 同步信息费,"
                  + "sum(result) as 回掉失败 "
                  + "from public_statistics as a WITH(NOLOCK) "
                  + "left join conduit as e on e.infoid=a.conduitid "
                  + "left join company as d on d.infoid=a.companyid "
                  + "where " + param
                  + "group by a.datatime order by a.datatime asc";

            }
            else if (Convert.ToInt32(datajson["index"]) == 2)
            {
                sql = "select f.name as 省份,tab.* from(select "
                  + "a.area,"
                  + "sum(upstream) as 上行总量,"
                  + "sum(verifycode) as 验证码总量,"
                  + "sum(singlecode) as 独立验证码,"
                  + "sum(hours72) as hours,"
                  + "sum(notifyall) as 通知订购总量,"
                  + "sum(singleuser) as 独立用户,"
                  + "sum(orderall) as 成功订购总量,"
                  + "sum(cancelall) as 退订总量,"
                  + "sum(a.amount) as 信息费,"
                  + "case when sum(orderall)>0 and sum(singlecode)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(orderall))/Convert(decimal(18,2),sum(singlecode))*100) when sum(upstream)>0 and sum(singlecode)=0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(orderall))/Convert(decimal(18,2),sum(upstream))*100) else 0 end as 转化率,"
                  + "sum(synccancel) as 同步退订总量,"
                  + "sum(syncorders) as 同步成功总量,"
                  + "case when sum(orderall)>0 then Convert(decimal(18,2),sum(syncorders)/Convert(decimal(18,2),sum(orderall))*100) else 0 end as 同步率,"
                  + "sum(syncamount) as 同步信息费,"
                  + "sum(result) as 回掉失败 "
                  + "from public_statistics as a WITH(NOLOCK) "
                  + "left join conduit as e on e.infoid=a.conduitid "
                  + "left join company as d on d.infoid=a.companyid "
                  + "where " + param
                  + " group by a.area) as tab full join areaInfo as f on f.name=tab.area "
                  + "where f.parent_id=0 order by f.sort asc";
            }
            else
            {
                sql ="select d.name as 公司名称,"
                  + "a.companyID,a.productID,a.conduitID,e.names as 通道,"
                  + "(select name from product where infoid=a.productID) as 产品名称,"
                  + "ISNULL(case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end,0) as 资费,"
                  + "sum(upstream) as 上行总量,"
                  + "sum(verifycode) as 验证码总量,"
                  + "sum(singlecode) as 独立验证码,"
                  + "sum(hours72) as hours,"
                  + "sum(notifyall) as 通知订购总量,"
                  + "sum(singleuser) as 独立用户,"
                  + "sum(orderall) as 成功订购总量,"
                  + "sum(cancelall) as 退订总量,"
                  + "sum(a.amount) as 信息费,"
                  + "case when sum(orderall)>0 and sum(singlecode)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(orderall))/Convert(decimal(18,2),sum(singlecode))*100) when sum(upstream)>0 and sum(singlecode)=0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(orderall))/Convert(decimal(18,2),sum(upstream))*100) else 0 end as 转化率,"
                  + "sum(synccancel) as 同步退订总量,"
                  + "sum(syncorders) as 同步成功总量,"
                  + "case when sum(orderall)>0 then Convert(decimal(18,2),sum(syncorders)/Convert(decimal(18,2),sum(orderall))*100) else 0 end as 同步率,"
                  + "sum(syncamount) as 同步信息费,"
                  + "sum(result) as 回掉失败 "
                  + "from public_statistics as a WITH(NOLOCK) "
                  + "left join conduit as e on e.infoid=a.conduitid "
                  + "left join company as d on d.infoid=a.companyid "
                  + "where " + param
                  + "group by a.companyID,a.conduitID,a.fee,d.name,e.names,a.productID order by d.name";
            }
            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";

            return info;
        }


        /// <summary>
        /// 获取目录数据
        /// </summary>
        /// <param name="node">目录节点</param>
        /// <returns></returns>
        public static string getTree(string node)
        {
            string info = null;
            string sql = "select infoid,0 as actionid,text,level,case when length>0 then 'false' else 'true' end as leaf,list,0 as pid from (select infoid,names as text,0 as level,(select count(infoid) from conduit where a.infoid=pid and flag=0) as length,list=STUFF((SELECT ','+convert(varchar(10),infoid) FROM conduit WHERE a.infoid=pid and flag=0 FOR XML PATH('')), 1, 1, '') from conduit as a where pid=0 and flag=0) as tab where length>0 order by text";
            if (!string.IsNullOrEmpty(node))
            {
                JObject json = JObject.Parse(node);
                if (Convert.ToInt32(json["level"]) == 0)
                    sql = "select infoid,0 as actionid,text,level,start,case when length>0 then 'false' else 'true' end as leaf,infoid as pid,sole from(select infoid,name as text,1 as level,startFlag as start,(select count(infoid) as length from action where productID=a.infoid) as length,sole from product as a where conduitid in(" + json["list"].ToString() + ")) as tab order by text";
                else if (Convert.ToInt32(json["level"]) == 1)
                    sql = "select companyid as infoid,0 as actionid,(select name from company where a.companyid=infoid) as text,2 as level,'true' as leaf,productid as pid,syncStart as start from action as a where productid=" + json["infoid"].ToString() + " group by companyid,productid,syncStart order by text";
                    //sql = "select b.infoid,a.infoid,b.name as text,2 as level,'true' as leaf,a.productID as pid from action as a left join company as b on b.infoid=a.companyID where a.productID=" + json["infoid"].ToString() + " group by b.infoid,b.name,a.productid order by text";
            }
            
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = JsonConvert.SerializeObject(dt, new DataTableConverter());

            return info;
        }



        public static string getConfigTree(string node)
        {
            string info = null;
            string sql = "select infoid,text,level,case when length>0 then 'false' else 'true' end as leaf from (select infoid,names as text,0 as level,(select count(infoid) from conduit where a.infoid=pid) as length from conduit as a where pid=0) as tab where length>0 order by text";
            if (!string.IsNullOrEmpty(node))
            {
                JObject json = JObject.Parse(node);

                if (Convert.ToInt32(json["level"]) == 0)
                    sql = "select infoid,text,level,case when length>0 then 'false' else 'true' end as leaf from(select infoid,names as text,1 as level,(select count(infoid) as length from product where conduitid=a.infoid) as length from conduit as a where pid=" + json["infoid"].ToString() + ") as tab order by text";
                else if (Convert.ToInt32(json["level"]) == 1)
                    sql = "select infoid,text,level,case when length>0 then 'false' else 'true' end as leaf from(select infoid,name as text,2 as level,(select count(infoid) as length from action where productID=a.infoid) as length from product as a where conduitid=" + json["infoid"].ToString() + ") as tab order by text";
                else if (Convert.ToInt32(json["level"]) == 2)
                    sql = "select b.infoid,b.name as text,3 as level,'true' as leaf from action as a left join company as b on b.infoid=a.companyID where a.productID=" + json["infoid"].ToString() + " group by b.infoid,b.name order by text";
            }

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = JsonConvert.SerializeObject(dt, new DataTableConverter());

            return info;
        }



        /// <summary>
        /// 获取业务规则数据
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public static string getJsonRules(int id)
        {
            return getJsonRules(id, null);
        }

        /// <summary>
        /// 获取业务规则数据
        /// </summary>
        /// <param name="id"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        public static string getJsonRules(int id, string field)
        {
            string info = null;
            string sql = "SELECT * FROM rules where infoid=" + id;
            if(!string.IsNullOrEmpty(field))
            {
                  sql = "SELECT "+field+" FROM rules where infoid=" + id;
               
                  DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                  if (dt.Rows.Count > 0)
                    info = dt.Rows[0][field].ToString();
            }
            else
            {
                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                if (dt.Rows.Count > 0)
                    info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
            }

            return info;
        }


        public static DataTable getProductRules(int productid, int actionid, string field)
        {
            DataTable dt = new DataTable();
            string sql = "SELECT * FROM rules where productid=" + productid;

            if (productid == 0)
                return dt;

            if (actionid > 0)
                sql += " and actionid= " + actionid;
            //else
               // sql += " and actionid=0";

            if (!string.IsNullOrEmpty(field))
                sql = sql.Replace("*", field);
            
            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
               
            return dt;
        }

        public static DataTable getProductRules(int productid, int companyid, int fee)
        {
            DataTable dt = new DataTable();
            string sql = string.Format("SELECT * FROM rules where productid={0} and companyid={1} and setfee={2}", productid,companyid,fee);

            if (productid == 0)
                return dt;

            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            return dt;
        }


        public static DataTable getProductRules(int infoid)
        {
            DataTable dt = new DataTable();
            string sql = string.Format("SELECT * FROM rules where infoid={0}", infoid);

            if (infoid == 0)
                return dt;

            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            return dt;
        }


        /// <summary>
        /// 获取规则省份
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static string getRuleArea(string node)
        {
            /*string area =null;
            string data = null;
            int ruleid=0;
            string allow = "'false'";
            if (infoid > 0)
            {
                DataTable proinfo = getProductInfo(infoid, 0);
                if (proinfo.Rows.Count > 0)
                {
                    JArray rule = JArray.Parse(proinfo.Rows[0]["ruleInfo"].ToString());
                    foreach (JObject item in rule)
                    {
                        int typeid = Convert.ToInt32(item["typeid"]);
                        if (typeid == 6)
                        {
                            ruleid = Convert.ToInt32(item["ruleid"]);
                            break;
                        }
                    }
                    area = getJsonRules(ruleid, "limit");
                    if (!string.IsNullOrEmpty(area))
                    {
                        JObject json = JObject.Parse(area);
                        allow = "case when charindex(name,'" + json["enable"].ToString() + "')>0 then 'false' else 'true' end";
                    }
                }
            }
            string sql = "select *," + allow + " as allow from (SELECT id,name FROM areaInfo where parent_id=0 and id not in(3412,3413)) as tab";

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            data = JsonConvert.SerializeObject(dt, new DataTableConverter());

            return data;*/

            return getRuleArea(node, false);
        }


       

        /*public static string getRuleArea(string node,bool flag)
        {

            string data = null;
            int ruleid = 0;
            string sql = null;
            string allow = "'false' as allow";
            DataTable dt = new DataTable();

            JObject info = JObject.Parse(node);
            int infoid = Convert.ToInt32(info["pid"]);
            if (infoid < 1)
            {
                sql = "SELECT id,name," + allow + " FROM areaInfo where parent_id=0 and id not in(3412,3413)";
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                data = JsonConvert.SerializeObject(dt, new DataTableConverter());
            }
            else
            {
                DataTable proinfo = getProductInfo(infoid, 0);
                if (proinfo.Rows.Count > 0)
                {
                    if (flag == false)
                    {
                        if (!string.IsNullOrEmpty(proinfo.Rows[0]["ruleInfo"].ToString()))
                        {
                            JArray rule = JArray.Parse(proinfo.Rows[0]["ruleInfo"].ToString());
                            foreach (JObject item in rule)
                            {
                                int typeid = Convert.ToInt32(item["typeid"]);
                                if (typeid == 6)
                                {
                                    ruleid = Convert.ToInt32(item["ruleid"]);
                                    break;
                                }
                            }
                            string area = getJsonRules(ruleid, "limit");
                            if (!string.IsNullOrEmpty(area))
                            {
                                JObject json = JObject.Parse(area);
                                allow = "case when charindex(name,'" + json["enable"].ToString() + "')>0 then 'false' else 'true' end as allow";
                            }
                            sql = "SELECT id,name," + allow + " FROM areaInfo where parent_id=0 and id not in(3412,3413)";
                            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                        }
                        else
                        {
                            sql = "SELECT id,name," + allow + " FROM areaInfo where parent_id=0 and id not in(3412,3413)";
                            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                        }
                        data = JsonConvert.SerializeObject(dt, new DataTableConverter());
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(proinfo.Rows[0]["ruleInfo"].ToString()))
                        {
                            JArray rule = JArray.Parse(proinfo.Rows[0]["ruleInfo"].ToString());
                            foreach (JObject item in rule)
                            {
                                int typeid = Convert.ToInt32(item["typeid"]);
                                if (typeid == 6)
                                {
                                    ruleid = Convert.ToInt32(item["ruleid"]);
                                    string area = getJsonRules(ruleid, "limit");
                                    if (!string.IsNullOrEmpty(area))
                                    {
                                        JObject json = JObject.Parse(area);
                                        allow = "case when charindex(name,'" + json["enable"].ToString() + "')>0 then 'true' else 'false' end as allow";
                                    }
                                    sql = "SELECT id,name," + allow + " FROM areaInfo where parent_id=0 and id not in(3412,3413)";
                                    if (Convert.ToInt32(info["level"]) == 2)
                                    {
                                        DataTable actioninfo = getAction(Convert.ToInt32(info["actionid"]), 0, 0);
                                        if (actioninfo.Rows.Count > 0)
                                        {

                                            JArray cprule = JArray.Parse(proinfo.Rows[0]["ruleInfo"].ToString());
                                            foreach (JObject cpitem in cprule)
                                            {
                                                int cptypeid = Convert.ToInt32(cpitem["typeid"]);
                                                if (cptypeid == 6)
                                                {
                                                    int cpruleid = Convert.ToInt32(cpitem["ruleid"]);
                                                    string cparea = getJsonRules(cpruleid, "limit");
                                                    if (!string.IsNullOrEmpty(cparea))
                                                    {
                                                        JObject json = JObject.Parse(cparea);
                                                        allow = "case when charindex(name,'" + json["disabled"].ToString() + "')>0 then 'fase' else 'true' end as allow";
                                                    }
                                                    sql = "select id,name," + allow + " from (" + sql + ") as tab where tab.allow='true'";
                                                }
                                            }
                                        }
                                    }
                                    dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                                }
                                else if (typeid == 2)
                                {
                                    dt.Columns.Add("day", typeof(string));
                                    dt.Columns.Add("dayfee", typeof(string));
                                    dt.Columns.Add("month", typeof(string));
                                    dt.Columns.Add("monthfee", typeof(string));
                                    ruleid = Convert.ToInt32(item["ruleid"]);
                                    string arearule = getJsonRules(ruleid, "limit");
                                    JArray arealist = JArray.Parse(arearule);
                                    foreach (JObject areaitem in arealist)
                                    {
                                        DataRow[] dr = dt.Select("name='" + areaitem["area"].ToString() + "'");
                                        if (dr.Length > 0)
                                        {
                                            dr[0]["day"] = areaitem["day"].ToString();
                                            dr[0]["dayfee"] = areaitem["dayfee"].ToString();
                                            dr[0]["month"] = areaitem["month"].ToString();
                                            dr[0]["monthfee"] = areaitem["monthfee"].ToString();
                                        }
                                    }
                                }
                            }
                        }
                        else
                        {
                            sql = "SELECT id,name,"+allow+" FROM areaInfo where parent_id=0 and id not in(3412,3413)";
                            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                        }
                        data = JsonConvert.SerializeObject(dt, new DataTableConverter());
       
                    }
                }
              }            
           
            return data;
        }*/



        public static string getRuleArea(string node, bool flag)
        {

            string data = null;

            string allow = "'false' as allow";
            string sql = "SELECT id,name," + allow + " FROM areaInfo where parent_id=0 and id not in(3412,3413)";
            DataTable dt = new DataTable();
            DataTable proinfo = new DataTable();
            JObject info = JObject.Parse(node);
            int productid = 0;
            int actionid = 0;

            if (Convert.ToInt32(info["level"]) == 2)
            {
               productid=Convert.ToInt32(info["pid"]);
               actionid = Convert.ToInt32(info["actionid"]);
            }
            else if (Convert.ToInt32(info["level"]) == 1)
                productid = Convert.ToInt32(info["infoid"]);

            else
            {
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                return JsonConvert.SerializeObject(dt, new DataTableConverter());
            }

            proinfo = getProductRules(productid, 0, null);

            if (proinfo.Rows.Count > 0)
            {
                DataRow[] ruleinfo = proinfo.Select("actionid=0 and typeid=6");
                DataRow[] arearule = proinfo.Select("actionid=0 and typeid=2");

                if (flag == false)
                {
                    if (ruleinfo.Length > 0)
                    {
                        JObject area = JObject.Parse(ruleinfo[0]["limit"].ToString());
                        allow = "case when charindex(name,'" + area["enable"].ToString() + "')>0 then 'false' else 'true' end as allow";
                        sql = "SELECT id,name," + allow + " FROM areaInfo where parent_id=0 and id not in(3412,3413)";

                        if (actionid > 0)
                        {
                            DataRow[] cpruleinfo = proinfo.Select("actionid=" + actionid + " and typeid=6");
                            if (cpruleinfo.Length > 0)
                            {
                                string cparea = cpruleinfo[0]["limit"].ToString();
                                JObject cpjson = JObject.Parse(cparea);
                                allow = "case when charindex(name,'" + cpjson["enable"].ToString() + "')>0 then 'false' else 'true' end as allow";
                                sql = "SELECT id,name," + allow + " FROM areaInfo where parent_id=0 and id not in(3412,3413)";
                            }
                        }
                    }
                }
                else
                {
                    if (ruleinfo.Length > 0)
                    {
                        JObject area = JObject.Parse(ruleinfo[0]["limit"].ToString());
                        allow = "case when charindex(name,'" + area["enable"].ToString() + "')>0 then 'true' else 'false' end as allow";
                        sql = "SELECT id,name," + allow + " FROM areaInfo where parent_id=0 and id not in(3412,3413)";

                        if (actionid > 0)
                        {
                            DataRow[] cpruleinfo = proinfo.Select("actionid=" + actionid + " and typeid=6");
                            DataRow[] cparearule = proinfo.Select("actionid=" + actionid + " and typeid=2");
                            if (cpruleinfo.Length > 0)
                            {
                                string cparea = cpruleinfo[0]["limit"].ToString();
                                JObject cpjson = JObject.Parse(cparea);
                                allow = "case when charindex(name,'" + cpjson["enable"].ToString() + "')>0 then 'true' else 'false' end as allow";
                            }

                            sql = "select id,name," + allow + " from (" + sql + ") as tab where tab.allow='true'";
                            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];



                            if (cparearule.Length > 0)
                            {
                                string areainfo = cparearule[0]["limit"].ToString();

                                dt.Columns.Add("day", typeof(string));
                                dt.Columns.Add("dayfee", typeof(string));
                                dt.Columns.Add("month", typeof(string));
                                dt.Columns.Add("monthfee", typeof(string));

                                if (!string.IsNullOrEmpty(areainfo))
                                {
                                    JArray arealist = JArray.Parse(areainfo);
                                    if (dt.Rows.Count > 0)
                                    {
                                        foreach (JObject areaitem in arealist)
                                        {
                                            DataRow[] dr = dt.Select("name='" + areaitem["area"].ToString() + "'");
                                            if (dr.Length > 0)
                                            {
                                                dr[0]["day"] = areaitem["day"].ToString();
                                                dr[0]["dayfee"] = areaitem["dayfee"].ToString();
                                                dr[0]["month"] = areaitem["month"].ToString();
                                                dr[0]["monthfee"] = areaitem["monthfee"].ToString();
                                            }
                                        }
                                    }
                                    /*else
                                    {

                                    }*/
                                }
                            }

                            return JsonConvert.SerializeObject(dt, new DataTableConverter());
                        }
                    }
                    else
                    {
                        if (actionid > 0)
                        {
                            DataRow[] cprule = proinfo.Select("actionid=" + actionid + " and typeid=6");
                            DataRow[] cparea = proinfo.Select("actionid=" + actionid + " and typeid=2");
                            JObject area = JObject.Parse(cprule[0]["limit"].ToString());
                            allow = "case when charindex(name,'" + area["enable"].ToString() + "')>0 then 'true' else 'false' end as allow";
                            sql = "SELECT id,name," + allow + " FROM areaInfo where parent_id=0 and id not in(3412,3413)";
                        }
                    }
                }
            }
            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            data = JsonConvert.SerializeObject(dt, new DataTableConverter());
            

            return data;
        }


    /// <summary>
    /// 获取省份和限量规则
    /// </summary>
    /// <param name="productid"></param>
    /// <param name="companyid"></param>
    /// <param name="fee"></param>
    /// <param name="flag">是否获取限量</param>
    /// <returns></returns>
       public static string getRuleAreax(int productid,int companyid,int fee, bool flag)
        {
            string infoid = null;
            string allow = "'false' as allow";
            string sql = "SELECT id,name," + allow + " FROM areaInfo where parent_id=0 and id not in(3412,3413)";
            DataTable proinfo = new DataTable();

            proinfo = getProductRules(productid, 0, 0);
            if(proinfo.Rows.Count>0)
            {
                DataRow[] arearule_p = proinfo.Select("companyid=0 and setfee=0 and typeid=6");
                
                DataRow[] feerule_p = proinfo.Select("companyid=0 and setfee=0 and typeid=2");
                
                if (fee > 0)
                {
                    DataRow[] arearule_f = proinfo.Select("companyid=" + companyid + " and setfee=" + fee + " and typeid=6");
                    if (arearule_f.Length > 0)
                    {
                        infoid = arearule_f[0]["infoid"].ToString();
                        JObject area = JObject.Parse(arearule_f[0]["limit"].ToString());
                        allow = "case when charindex(name,'" + area["enable"].ToString() + "')>0 then 'true' else 'false' end as allow";
                        sql = "SELECT id,name," + allow + " FROM areaInfo where parent_id=0 and id not in(3412,3413)";
                    }
                }
                else if (companyid > 0)
                {
                    DataRow[] arearule_c = proinfo.Select("companyid=" + companyid + " and setfee=0 and typeid=6");
                    if (arearule_c.Length > 0)
                    {
                        infoid = arearule_c[0]["infoid"].ToString();
                        JObject area = JObject.Parse(arearule_c[0]["limit"].ToString());
                        allow = "case when charindex(name,'" + area["enable"].ToString() + "')>0 then 'true' else 'false' end as allow";
                        sql = "SELECT id,name," + allow + " FROM areaInfo where parent_id=0 and id not in(3412,3413)";
                    }
                }
                else if (arearule_p.Length > 0)
                {
                    infoid = arearule_p[0]["infoid"].ToString();
                    JObject area = JObject.Parse(arearule_p[0]["limit"].ToString());
                    allow = "case when charindex(name,'" + area["enable"].ToString() + "')>0 then 'true' else 'false' end as allow";
                    sql = "SELECT id,name," + allow + " FROM areaInfo where parent_id=0 and id not in(3412,3413)";
                }

                DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
                dt.Columns.Add("day", typeof(Int32));
                dt.Columns.Add("max", typeof(Int32));
                if (flag)
                {
                    if (dt.Rows.Count > 0)
                    {
                        if (fee > 0)
                        {
                            DataRow[] feerule_f = proinfo.Select("companyid=" + companyid + " and setfee=" + fee + " and typeid=2");
                            if (feerule_f.Length > 0)
                            {
                                //infoid += ","+feerule_f[0]["infoid"].ToString();
                                JObject areafee = JObject.Parse(feerule_f[0]["limit"].ToString());
                                JArray data = JArray.Parse(areafee["data"].ToString());
                                foreach (JObject items in data)
                                {
                                    DataRow[] province = dt.Select("name like '" + items["area"] + "%'");
                                    if (province.Length > 0)
                                        province[0]["day"] = Convert.ToInt32(items["day"]);
                                }
                                return "{\"2\":" + feerule_f[0]["infoid"].ToString() + ",\"6\":" + infoid + ",\"field\":\"" + areafee["field"].ToString() + "\",\"filter\":" + areafee["filter"].ToString().ToLower() + ",\"step\":" + Convert.ToInt32(areafee["step"]) + ",\"data\":" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";
                            }
                         }
                        else if (companyid > 0)
                        {
                            DataRow[] feerule_c = proinfo.Select("companyid=" + companyid + " and setfee=0 and typeid=2");
                            if (feerule_c.Length > 0)
                            {
                                //infoid += "," + feerule_c[0]["infoid"].ToString();
                                JObject areafee = JObject.Parse(feerule_c[0]["limit"].ToString());
                                JArray data = JArray.Parse(areafee["data"].ToString());
                                foreach (JObject items in data)
                                {
                                    DataRow[] province = dt.Select("name like '" + items["area"] + "%'");
                                    if (province.Length > 0)
                                        province[0]["day"] = Convert.ToInt32(items["day"]);
                                }
                                return "{\"2\":" + areafee["field"].ToString() + ",\"6\":" + infoid + ",\"filter\":" + areafee["filter"].ToString().ToLower() + ",\"step\":" + Convert.ToInt32(areafee["step"]) + ",\"data\":" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";
                            }
                        }
                        else if (feerule_p.Length > 0)
                        {
                            //infoid += "," + feerule_p[0]["infoid"].ToString();
                            JObject areafee = JObject.Parse(feerule_p[0]["limit"].ToString());
                            JArray data = JArray.Parse(areafee["data"].ToString());
                            foreach (JObject items in data)
                            {
                                DataRow[] province = dt.Select("name like '" + items["area"] + "%'");
                                if (province.Length > 0)
                                    province[0]["day"] = Convert.ToInt32(items["day"]);
                            }
                            return "{\"6\": " + infoid + " ,\"2\":" + feerule_p[0]["infoid"].ToString() + ",\"field\":\"" + areafee["field"].ToString() + "\",\"filter\":" + areafee["filter"].ToString().ToLower() + ",\"step\":" + Convert.ToInt32(areafee["step"]) + ",\"data\":" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";
                        }
                    }
                }
                return "{\"6\":\"" + infoid + "\",\"data\":" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";
            }
            DataTable dta = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            return "{\"data\":" + JsonConvert.SerializeObject(dta, new DataTableConverter()) + "}";
        }


        /// <summary>
        /// 获取产品数据
        /// </summary>
        /// <param name="infoid"></param>
        /// <param name="conduitid"></param>
        /// <returns></returns>
        public static DataTable getProductInfo(int infoid,int conduitid)
        {
            DataTable dt = new DataTable() ;
            string sql = "SELECT * FROM product where infoid=" + infoid;
            
            if(conduitid>0)
                sql = "SELECT * FROM product where conduitid=" + conduitid+" order by infoid";

            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            return dt;
        }


        public static DataTable getProductInfo(int infoid)
        {
            DataTable dt = new DataTable();
            string sql = "SELECT * FROM product where infoid=" + infoid;

            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            return dt;
        }



        public static DataTable getAction(int infoid, int productid, int companyid)
        {
            DataTable dt = new DataTable();
            string param = string.Empty;
            string sql = "SELECT a.*,(select name from company where infoid=a.companyid) as companyname FROM action as a where a.infoid=" + infoid;

            if (infoid == 0)
            {
                if (productid > 0)
                    param += " and a.productid=" + productid;
                if (companyid > 0)
                    param += " and a.companyid=" + companyid;

                sql = "SELECT a.*,(select name from company where infoid=a.companyid) as companyname FROM action as a where a.infoid>0 " + param;
            }
          
            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            return dt;
        }


        public static DataTable getAction(int productid, string companyid)
        {
            DataTable dt = new DataTable();
            string param = string.Empty;
            string sql = "SELECT a.*,(select name from company where infoid=a.companyid) as companyname FROM action as a where a.productid=" + productid + " and a.companyid in(" + companyid+")";
            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            return dt;
        }

        public static DataTable getAction(int productid)
        {
            DataTable dt = new DataTable();
            string param = string.Empty;
            string sql = "SELECT a.*,(select name from company where infoid=a.companyid) as companyname FROM action as a where a.productid=" + productid;
            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            return dt;
        }

        public static DataTable getAction(string idlist)
        {
            DataTable dt = new DataTable();
            string param = string.Empty;
            string sql = "SELECT a.*,(select name from company where infoid=a.companyid) as companyname FROM action as a where a.infoid in("+idlist+")";
            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            return dt;
        }

        /// <summary>
        /// 获取同步地址
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static string getSyncUrl(int infoid)
        {
            string data = string.Empty;
            DataTable dt = getAction(infoid,0,0);
            if (dt.Rows.Count > 0)
                data = dt.Rows[0]["syncUrl"].ToString();
            return data;
        }


        /// <summary>
        /// 获取扣量值
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static int getPoint(int infoid)
        {
            int data = 0;
            DataTable dt = getAction(infoid, 0, 0);
            if (dt.Rows.Count > 0)
                data = Convert.ToInt32(dt.Rows[0]["point"]);
            return data;
        }


        /// <summary>
        /// 获取下游分成比率
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static int getCPdivideInto(int infoid)
        {
            int data = 0;
            DataTable dt = getAction(infoid, 0, 0);
            if (dt.Rows.Count > 0)
                data = Convert.ToInt32(dt.Rows[0]["divideInto"]);
            return data;
        }


        /// <summary>
        /// 获取上游分成比率
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static int getPDdivideInto(int infoid)
        {
            int data = 0;

            DataTable dt = getProductInfo(infoid);
            if (dt.Rows.Count > 0)
                data = Convert.ToInt32(dt.Rows[0]["divideInto"]);
            return data;
        }

        /// <summary>
        /// 获取下游结算方式
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static int getSettlement(int infoid)
        {
            int data = 0;
            DataTable dt = getAction(infoid, 0, 0);
            if (dt.Rows.Count > 0)
                data = Convert.ToInt32(dt.Rows[0]["settlement"]);
            return data;
        }

        /// <summary>
        /// 回到失败
        /// </summary>
        /// <param name="pagesize"></param>
        /// <param name="currentpage"></param>
        /// <param name="param"></param>
        /// <param name="node"></param>
        /// <returns></returns>
        public static string getNotifyFail(int pagesize, int currentpage, string param, string node)
        {
            string info = "{data:[],total:0}";
            string sql = null;
            string paramstr = "";
            string include = "";
            string datatime = null;

            Time tm = new Time();
            datatime = " and a.datatime between " + tm.GetToday();

            if (!string.IsNullOrEmpty(param))
            {
                JObject datajson = JObject.Parse(param);

                string[] items = datajson["items"].ToString().Replace(" ", ",").Split(',');


                if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
                {
                    string stime = datajson["stime"].ToString();
                    string etime = datajson["etime"].ToString();

                    if (stime.Length < 5)
                        datajson["stime"] = "0" + stime;
                    if (etime.Length < 5)
                        datajson["etime"] = "0" + etime;

                    datatime = " and a.datatime between '" + datajson["sdate"].ToString() + " " + datajson["stime"] + "' and  DATEADD(MINUTE,+1,'" + datajson["edate"].ToString() + " " + datajson["etime"] + "')";
                }

                for (int i = 0; i < items.Length; i++)
                {
                    if (!string.IsNullOrEmpty(items[i].Trim()))
                        include += ",'" + items[i].Trim() + "'";
                }

                if (!string.IsNullOrEmpty(include.Trim()))
                {
                    if (Convert.ToInt32(datajson["index"]) == 1)
                        paramstr = " and imsi in(" + include.Substring(1) + ") ";
                    else if (Convert.ToInt32(datajson["index"]) == 2)
                        paramstr = " and mobile in(" + include.Substring(1) + ") ";
                    else if (Convert.ToInt32(datajson["index"]) == 3)
                        paramstr = " and userOrder in(" + include.Substring(1) + ") ";
                }


                if (!string.IsNullOrEmpty(datajson["optype"].ToString().Trim()))
                {
                    if (datajson["optype"].ToString().IndexOf("[") == -1)
                        paramstr += " and a.optype=" + datajson["optype"].ToString();
                }

                if (null != datajson["area"] && !string.IsNullOrEmpty(datajson["area"].ToString()))
                {
                    string area = "";
                    if (datajson["area"].ToString().IndexOf("[") != -1)
                    {
                        JArray lsa = JArray.Parse(datajson["area"].ToString());

                        for (int i = 0; i < lsa.Count; i++)
                            area += "or a.area like '" + lsa[i].ToString() + "%' ";

                        paramstr += " and (" + area.Substring(2) + ") ";
                    }
                    else
                        paramstr += "and a.area like '" + datajson["area"].ToString() + "%' ";
                }

                if (!string.IsNullOrEmpty(node))
                {
                    JObject json = JObject.Parse(node);
                    if (json["level"].ToString() == "0")
                        paramstr += " and a.conduitID in (" + json["list"].ToString() + ")";
                    else if (json["level"].ToString() == "1")
                        paramstr += " and a.productID=" + json["infoid"].ToString();
                    else if (json["level"].ToString() == "2")
                        paramstr += " and a.productID=" + json["pid"].ToString() + " and a.companyID=" + json["infoid"].ToString();
                }

            }
            int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "SELECT COUNT(a.infoid) FROM public_sync_2017 as a WITH(NOLOCK) WHERE a.result>0" + datatime + paramstr));
            if (currentpage == 1)
                sql = "SELECT TOP " + pagesize + " a.infoid,a.companyid,b.name,c.name as product,a.mobile,a.imsi,"
                    + "case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else a.fee end as fee,a.area,c.conduitid,"
                    + "(CASE WHEN a.OPType=0 THEN '定购'  WHEN a.OPType=1 THEN '退订'  END) optype,"
                    + "(CASE WHEN a.buyid=1 THEN '点播' ELSE '包月' END) buyid,"
                    + "(CASE WHEN a.resultCode=0 THEN '成功' END) status,"
                    + "(select CASE WHEN count(infoid)>0 THEN 1 END from blacklist where mobile=a.mobile) balck,"
                    + "CONVERT(varchar(21), a.datatime, 20) ordered"
                    + " FROM public_sync_2017 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyid"
                    + " right join product as c on c.infoid=a.productID"
                    + " where a.result>0 " + datatime + paramstr + " ORDER BY a.datatime DESC";
            else
            {
                sql = "SELECT TOP " + pagesize + " a.infoid,a.companyid,b.name ,c.name as product,a.mobile,a.imsi,"
                    + "case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else a.fee end as fee,a.area,c.conduitid,"
                    + "(CASE WHEN a.OPType=0 THEN '定购' WHEN a.OPType=1 THEN '退订'  END) optype,"
                    + "(CASE WHEN a.buyid=1 THEN '点播' ELSE '包月' END) buyid,"
                    + "(CASE WHEN a.resultCode=0 THEN '成功' END) status,"
                    + "(select CASE WHEN count(infoid)>0 THEN 1 END from blacklist as b where mobile=a.mobile) balck,"
                    + "CONVERT(varchar(21), a.datatime, 20) ordered"
                    + " FROM public_sync_2017 as a WITH(NOLOCK) left join company as b on b.infoid=a.companyid"
                    + " right join product as c on c.infoid=a.productID WHERE a.datatime <=(SELECT MIN(datatime)"
                    + " FROM (SELECT TOP " + ((currentpage - 1) * pagesize + 1) + " [datatime] FROM public_sync_2017 WITH(NOLOCK)"
                    + " WHERE result>0" + datatime.Replace("a.", "") + paramstr.Replace("a.", "") + " ORDER BY [datatime] DESC) AS [tblTmp]) and a.result>0 "
                    + datatime + paramstr + " ORDER BY a.datatime DESC";
            }
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",total:" + TotalRecords + "}";


            return info;
        }


        public static string getMultiServiceData(string search,string node)
        {
            string info = null;
            string sql = null;
            string sql1 = null;
            string param = "";
            string param1 = "";
            string param2 = "";
            JObject datajson = JObject.Parse(search);
            DataTable dt = new DataTable();
            /*string stime = datajson["stime"].ToString();
            string etime = datajson["etime"].ToString();
            
            if (stime.Length < 5)
                datajson["stime"] = "0" + stime;
            if (etime.Length < 5)
                datajson["etime"] = "0" + etime;*/


            if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
            {

                string eday = datajson["edate"].ToString();
                string[] temp = eday.Split('-');
                if (temp[1].Length < 2)
                    temp[1] = "0" + temp[1];
                if (temp[2].Length < 2)
                    temp[2] = "0" + temp[2];
                datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];
                param += " and a.datatime between '" + datajson["sdate"].ToString() + "' and  DATEADD(DAY,+1,'" + datajson["edate"].ToString()+ "')";
                param2 = param;

            }

            /*if (!string.IsNullOrEmpty(datajson["conduit"].ToString()))
            {
                if (Convert.ToInt32(datajson["conduit"]) > 0)
                {
                    param += " and a.conduitID=" + datajson["conduit"].ToString();
                    param1 += " and a.conduitID=" + datajson["conduit"].ToString();
                    param2 += " and a.conduitID=" + datajson["conduit"].ToString();
                }
            }

            if (!string.IsNullOrEmpty(datajson["company"].ToString()))
            {
                if (Convert.ToInt32(datajson["company"]) > 0)
                {
                    param += " and a.companyID=" + datajson["company"].ToString();
                    param1 += " and a.companyID=" + datajson["company"].ToString();
                }
            }

            if (!string.IsNullOrEmpty(datajson["product"].ToString()))
            {
                if (Convert.ToInt32(datajson["product"]) > 0)
                {
                    param += " and a.productID=" + datajson["product"].ToString();
                    param1 += " and a.productID=" + datajson["product"].ToString();
                    param2 += " and a.productID=" + datajson["product"].ToString();
                }
            }*/

            if (!string.IsNullOrEmpty(node))
            {
                JObject json = JObject.Parse(node);
                if (json["level"].ToString() == "0")
                {
                    param += " and a.conduitID in (" + json["list"].ToString() + ")";
                    param1 += " and a.conduitID in (" + json["list"].ToString() + ")";
                    param2 += " and a.conduitID in (" + json["list"].ToString() + ")";
                }
                else if (json["level"].ToString() == "1")
                {
                    param += " and a.productID=" + json["infoid"].ToString();
                    param1 += " and a.productID in (" + json["infoid"].ToString() + ")";
                    param2 += " and a.productID in (" + json["infoid"].ToString() + ")";
                }
                else if (json["level"].ToString() == "2")
                {
                    param += " and a.productID=" + json["pid"].ToString() + " and a.companyID=" + json["infoid"].ToString();
                    param1 += " and a.productID=" + json["pid"].ToString() + " and a.companyID=" + json["infoid"].ToString();
                    param2 += " and a.productID=" + json["pid"].ToString() + " and a.companyID=" + json["infoid"].ToString();
                }
            }

            if (Convert.ToInt32(datajson["index"]) == 1)
            {

                sql = "select convert(char(10),a.datatime,120) as 日期,"
                      + "0 as 上行总量,"
                      + "0 as 验证码总量,"
                      + "0 as 独立验证码,"
                      + "0 as hours,"
                      + "sum(case when a.optype<>1 then 1 else 0 end) as 通知订购总量,"
                      //+ "COUNT(DISTINCT(case when a.optype<>1 then (case when len(a.mobile)>0 then a.mobile else a.imsi end) end)) as 独立用户,"
                      + "sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end) as 成功订购总量,"
                      + "COUNT(DISTINCT(case when a.resultCode=0 and a.optype=0 then (case when len(a.userOrder)>0 then a.userOrder else a.mobile end) end)) as 独立用户,"
                      + "sum(case when a.resultCode=0 and a.optype=1 then 1 else 0 end) as 退订总量,"
                      + "sum(case when a.resultCode=0 and a.optype=0 then case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end else 0 end) as 有效信息费,"
                      + "sum(case when c.resultCode=0 and c.optype=1 then 1 else 0 end) as 同步退订总量,"
                      + "sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end) as 同步成功总量,"
                      + "case when sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when a.optype<>1 then 1 else 0 end))*100) else 0 end as 转化率,"
                      + "case when sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end))/ Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))*100) else 0 end as 同步率,"
                      + "sum(case when c.resultCode=0 and c.optype=0 and c.result=0 then case when len(c.fee)>2 then Convert(decimal(18,2),c.fee)/100 else Convert(decimal(18,2),c.fee) end else 0 end) as 同步有效信息费,"
                      + "sum(case when c.result=1 and c.optype=0 then 1 else 0 end) as 回掉失败 "
                      + "from public_notify_2017 as a WITH(NOLOCK) "
                      + "left join public_sync_2017 as c WITH(NOLOCK) on c.infoid=a.infoid "
                      + "where a.infoid>0" + param
                      + " group by convert(char(10),a.datatime,120)";

                string sqla = "select convert(char(10),datatime,23) as 日期,sum(case when code is null or code='' then 1 else 0 end) as 上行总量,COUNT(DISTINCT(case when buyid=2 and len(IMSI)>0 and len(code)>0 then IMSI when buyid=2 and len(mobile)>0 and len(code)>0 then mobile end))+COUNT(case when buyid=1 and len(IMSI)>0 and len(code)>0 then IMSI when buyid=1 and len(mobile)>0 and len(code)>0 then mobile end) as 独立验证码, COUNT(case when len(code)>0 and len(IMSI)>0 then IMSI when len(code)>0 and len(mobile)>0 then mobile end) as 验证码总量 from public_order_2017 WITH(NOLOCK) where infoid>0" + param.Replace("a.", "") + " group by convert(char(10),datatime,23)";

                DataTable dt1 = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sqla).Tables[0];

                sql1 = "select count(a.infoid) from public_notify_2017 as a WITH(NOLOCK) where (select count(e.infoid) from public_notify_2017 as e WITH(NOLOCK) where e.resultCode=0 and e.buyID=2 and e.optype=1 and e.mobile=a.mobile and CONVERT(varchar(30),e.datatime,120)<CONVERT(varchar(30),DATEADD(hh,+72,a.datatime),120)" + param1.Replace("a.", "e.") + ")>0 and a.resultCode=0 and a.buyID=2 and a.optype=0 and a.datatime between '{0}' and CONVERT(varchar(10),DATEADD(hh,+24,'{0}'),120)" + param1;

                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

                if (dt1.Rows.Count > 0)
                {
                    for (int i = 0; i < dt1.Rows.Count; i++)
                    {

                        DataRow[] temp1 = dt.Select("日期='" + dt1.Rows[i]["日期"].ToString() + "'");

                        if (temp1.Length > 0)
                        {
                            temp1[0]["上行总量"] = Convert.ToInt32(dt1.Rows[i]["上行总量"]);
                            temp1[0]["验证码总量"] = Convert.ToInt32(dt1.Rows[i]["验证码总量"]);
                            temp1[0]["独立验证码"] = Convert.ToInt32(dt1.Rows[i]["独立验证码"]);
                            double code = Convert.ToDouble(dt1.Rows[i]["独立验证码"]);
                            double order = Convert.ToDouble(dt1.Rows[i]["上行总量"]);
                            double notify = Convert.ToDouble(temp1[0]["通知订购总量"]);
                            double result = Convert.ToDouble(temp1[0]["成功订购总量"]);

                            if (code > 0)
                            {
                                double num1 = (result / code);
                                temp1[0]["转化率"] = Math.Round(num1 * 100, 2);
                            }

                        }
                        else
                        {
                            DataRow dRow = dt.NewRow();
                            dRow["日期"] = dt1.Rows[i]["日期"].ToString();
                            dRow["上行总量"] = Convert.ToInt32(dt1.Rows[i]["上行总量"]);
                            dRow["验证码总量"] = Convert.ToInt32(dt1.Rows[i]["验证码总量"]);
                            dRow["独立验证码"] = Convert.ToInt32(dt1.Rows[i]["独立验证码"]);
                            dt.Rows.Add(dRow);
                        }


                    }
                }
                dt.DefaultView.Sort = "日期 DESC";
                dt = dt.DefaultView.ToTable();

                foreach (DataRow dr in dt.Rows)
                {
                    dr["hours"] = get72Hours(string.Format(sql1, dr["日期"].ToString()));
                }


            }
            else if (Convert.ToInt32(datajson["index"]) == 2)
            {
                sql = "WITH x4 AS ("
                    + "select (case when SUBSTRING(a.area,0,CHARINDEX(' ',a.area)) is null then '[无号码]' when SUBSTRING(a.area,0,CHARINDEX(' ',a.area))='' then '[未识别]' else SUBSTRING(a.area,0,CHARINDEX(' ',a.area)) end) as 省份,"
                    + "0 as 上行总量,"
                    + "0 as 验证码总量,"
                    + "0 as 独立验证码,"
                    + "0 as hours,"
                    + "sum(case when a.optype<>1 then 1 else 0 end) as 通知订购总量,"
                    //+ "COUNT(DISTINCT(case when a.optype<>1 then (case when len(a.mobile)>0 then a.mobile else a.imsi end) end)) as 独立用户,"
                    + "sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end) as 成功订购总量,"
                    + "COUNT(DISTINCT(case when a.resultCode=0 and a.optype=0 then (case when len(a.userOrder)>0 then a.userOrder else a.mobile end) end)) as 独立用户,"
                    + "sum(case when a.resultCode=0 and a.optype=1 then 1 else 0 end) as 退订总量,"
                    + "sum(case when a.resultCode=0 and a.optype=0 then case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end else 0 end) as 有效信息费,"
                    + "case when sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when a.optype<>1 then 1 else 0 end))*100) else 0 end as 转化率,"
                    + "sum(case when c.resultCode=0 and c.optype=1 then 1 else 0 end) as 同步退订总量,"
                    + "sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end) as 同步成功总量,"
                    + "case when sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))*100) else 0 end as 同步率,"
                    + "sum(case when c.resultCode=0 and c.optype=0 and c.result=0 then case when len(c.fee)>2 then Convert(decimal(18,2),c.fee)/100 else Convert(decimal(18,2),c.fee) end else 0 end) as 同步有效信息费,"
                    + "sum(case when c.result=1 and c.optype=0 then 1 else 0 end) as 回掉失败 "
                    + "from public_notify_2017 as a WITH(NOLOCK) "
                    + "left join public_sync_2017 as c WITH(NOLOCK) on c.infoid=a.infoid "
                    + "where a.infoid>0" + param
                    + " group by SUBSTRING(a.area,0,CHARINDEX(' ',a.area))"
                    + ") "
                    + "select a.name as 省份,x4.上行总量,x4.验证码总量,x4.独立验证码,x4.通知订购总量,x4.独立用户,x4.成功订购总量,x4.退订总量,x4.hours,x4.有效信息费,x4.转化率,x4.同步退订总量,x4.同步成功总量,x4.同步率,x4.同步有效信息费,x4.回掉失败 from areaInfo as a left join x4 on x4.省份=a.name where a.parent_id=0 order by a.sort";

                string sqla = "select (case when SUBSTRING(area,0,CHARINDEX(' ',area)) is null then '[无号码]' when SUBSTRING(area,0,CHARINDEX(' ',area))='' then '[未识别]' else SUBSTRING(area,0,CHARINDEX(' ',area)) end) as 省份,sum(case when code is null or code='' then 1 else 0 end) as 上行总量, COUNT(DISTINCT(case when buyid=2 and len(IMSI)>0 and len(code)>0 then IMSI when buyid=2 and len(mobile)>0 and len(code)>0 then mobile end))+COUNT(case when buyid=1 and len(IMSI)>0 and len(code)>0 then IMSI when buyid=1 and len(mobile)>0 and len(code)>0 then mobile end) as 独立验证码, COUNT(case when len(code)>0 and len(IMSI)>0 then IMSI when len(code)>0 and len(mobile)>0 then mobile end) as 验证码总量 from public_order_2017 WITH(NOLOCK) where infoid>0 " + param.Replace("a.", "") + " group by SUBSTRING(area,0,CHARINDEX(' ',area))";
                DataTable dt1 = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sqla).Tables[0];


                sql1 = "select count(a.infoid) from public_notify_2017 as a WITH(NOLOCK) where (select count(e.infoid) from public_notify_2017 as e WITH(NOLOCK) where e.resultCode=0 and e.buyID=2 and e.optype=1 and SUBSTRING(e.area,0,CHARINDEX(' ',e.area))='{0}' and e.mobile=a.mobile and CONVERT(varchar(30),e.datatime,120)<CONVERT(varchar(30),DATEADD(hh,+72,a.datatime),120)" + param1.Replace("a.", "e.") + ")>0 and a.resultCode=0 and a.buyID=2 and a.optype=0 and SUBSTRING(a.area,0,CHARINDEX(' ',a.area))='{0}'" + param;
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

                if (dt1.Rows.Count > 0)
                {
                    for (int i = 0; i < dt1.Rows.Count; i++)
                    {

                        DataRow[] temp1 = dt.Select("省份='" + dt1.Rows[i]["省份"].ToString() + "'");

                        if (temp1.Length > 0)
                        {
                            temp1[0]["上行总量"] = Convert.ToInt32(dt1.Rows[i]["上行总量"]);
                            temp1[0]["验证码总量"] = Convert.ToInt32(dt1.Rows[i]["验证码总量"]);
                            temp1[0]["独立验证码"] = Convert.ToInt32(dt1.Rows[i]["独立验证码"]);

                            double code = Convert.ToDouble(dt1.Rows[i]["独立验证码"]);
                            double order = Convert.ToDouble(dt1.Rows[i]["上行总量"]);
                            if (!string.IsNullOrEmpty(temp1[0]["通知订购总量"].ToString()))
                            {
                                double notify = Convert.ToDouble(temp1[0]["通知订购总量"].ToString());
                                double result = Convert.ToDouble(temp1[0]["成功订购总量"].ToString());

                                if (code > 0)
                                {
                                    double num1 = result / code;
                                    temp1[0]["转化率"] = Math.Round(num1 * 100, 2);
                                }
                            }
                        }
                        else
                        {
                            DataRow dRow = dt.NewRow();
                            dRow["省份"] = dt1.Rows[i]["省份"].ToString();
                            dRow["上行总量"] = Convert.ToInt32(dt1.Rows[i]["上行总量"]);
                            dRow["验证码总量"] = Convert.ToInt32(dt1.Rows[i]["验证码总量"]);
                            dRow["独立验证码"] = Convert.ToInt32(dt1.Rows[i]["独立验证码"]);

                            dt.Rows.Add(dRow);
                        }

                    }
                }

                /*foreach (DataRow dr in dt.Rows)
                {
                    dr["hours"] = get72Hours(string.Format(sql1, dr["省份"].ToString()));
                }*/

            }
            else
            {
                sql ="select d.name as 公司名称,"
                  + "a.companyID,a.productID,a.conduitID,e.names as 通道,"
                  + "(select name from product where infoid=a.productID) as 产品名称,"
                  + "ISNULL(case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end,0) as 资费,"
                  + "0 as 上行总量,"
                  + "0 as 验证码总量,"
                  + "0 as 独立验证码,"
                  + "0 as hours,"
                  + "sum(case when a.optype<>1 then 1 else 0 end) as 通知订购总量,"
                  //+ "COUNT(DISTINCT(case when a.optype<>1 then a.mobile end)) as 独立用户,"
                  + "sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end) as 成功订购总量,"
                  + "COUNT(DISTINCT(case when a.resultCode=0 and a.optype=0 then (case when len(a.userOrder)>0 then a.userOrder else a.mobile end) end)) as 独立用户,"
                  + "sum(case when a.resultCode=0 and a.optype=1 then 1 else 0 end) as 退订总量,"
                  + "sum(case when a.resultCode=0 and a.optype=0 then case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end else 0 end) as 有效信息费,"
                  + "case when sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when a.optype<>1 then 1 else 0 end))*100) else 0 end as 转化率,"
                  + "sum(case when c.resultCode=0 and c.optype=1 then 1 else 0 end) as 同步退订总量,"
                  + "sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end) as 同步成功总量,"
                  + "case when sum(case when c.resultCode=0 and a.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end))/Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))*100) else 0 end as 同步率,"
                  + "sum(case when c.resultCode=0 and c.result=0 and c.optype=0 then case when len(c.fee)>2 then Convert(decimal(18,2),c.fee)/100 else Convert(decimal(18,2),c.fee) end else 0 end) as 同步有效信息费,"
                  + "sum(case when c.result=1 and c.optype=0 then 1 else 0 end) as 回掉失败 "
                  + "from public_sync_2017 as c WITH(NOLOCK) "
                  + "right join public_notify_2017 as a WITH(NOLOCK) on c.infoid=a.infoid "
                  + "left join conduit as e on e.infoid=a.conduitid "
                  + "left join company as d on d.infoid=a.companyid "

                  + "where d.infoid>0" + param
                  + " group by a.companyID,a.conduitID,a.fee,d.name,e.names,a.productID";

                if (string.IsNullOrEmpty(param2))
                    sql1 = "select count(a.infoid) from public_notify_2017 as a where (select count(e.infoid) from public_notify_2017 as e where e.resultCode=0 and e.buyID=2 and e.optype=1 and e.mobile=a.mobile and CONVERT(varchar(30),e.datatime,120)<CONVERT(varchar(30),DATEADD(hh,+72,a.datatime),120) and e.fee={0} and e.companyID={1})>0 and a.resultCode=0 and a.buyID=2 and a.optype=0 and a.fee={0} and a.companyID={1}";
                else
                    sql1 = "select count(a.infoid) from public_notify_2017 as a where (select count(e.infoid) from public_notify_2017 as e where e.resultCode=0 and e.buyID=2 and e.optype=1 and e.mobile=a.mobile and CONVERT(varchar(30),e.datatime,120)<CONVERT(varchar(30),DATEADD(hh,+72,a.datatime),120) and e.fee={0} and e.companyID={1}" + param2.Replace("a.", "e.") + ")>0 and a.resultCode=0 and a.buyID=2 and a.optype=0 and a.fee={0} and a.companyID={1}" + param2;


                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];


                string sqla = "select (select top 1 names from conduit where infoid=a.conduitid) as names,(select top 1 name from company where infoid=a.companyid) as name,a.companyID,a.conduitID,a.productID,case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end as fee,(select name from product where infoid=a.productID) as 产品名称,sum(case when a.code is null or a.code='' then 1 else 0 end) as 上行总量, COUNT(DISTINCT(case when a.buyid=2 and len(a.IMSI)>0 and len(a.code)>0 then a.IMSI when a.buyid=2 and len(a.mobile)>0 and len(a.code)>0 then a.mobile end))+COUNT(case when a.buyid=1 and len(a.IMSI)>0 and len(a.code)>0 then a.IMSI when a.buyid=1 and len(a.mobile)>0 and len(a.code)>0 then a.mobile end) as 独立验证码, COUNT(case when len(a.code)>0 and len(a.IMSI)>0 then a.IMSI when len(a.code)>0 and len(a.mobile)>0 then a.mobile end) as 验证码总量 from public_order_2017 as a WITH(NOLOCK) where a.infoid>0" + param + " group by a.companyID,a.conduitID,a.productID,a.fee";
                DataTable dt1 = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sqla).Tables[0];

                if (dt1.Rows.Count > 0)
                {
                    for (int i = 0; i < dt1.Rows.Count; i++)
                    {
                        DataRow[] temp1 = dt.Select("companyID=" + dt1.Rows[i]["companyID"].ToString() + " and conduitID=" + dt1.Rows[i]["conduitID"].ToString() + " and productID=" + dt1.Rows[i]["productID"].ToString() + " and 资费=" + dt1.Rows[i]["fee"].ToString());


                        if (temp1.Length > 0)
                        {

                            double code = Convert.ToDouble(dt1.Rows[i]["独立验证码"]);
                            double order = Convert.ToDouble(dt1.Rows[i]["上行总量"]);
                            double notify = Convert.ToDouble(temp1[0]["通知订购总量"]);
                            double result = Convert.ToDouble(temp1[0]["成功订购总量"]);
                            temp1[0]["上行总量"] = Convert.ToInt32(dt1.Rows[i]["上行总量"]);
                            temp1[0]["验证码总量"] = Convert.ToInt32(dt1.Rows[i]["验证码总量"]);
                            temp1[0]["独立验证码"] = Convert.ToInt32(dt1.Rows[i]["独立验证码"]);
                            if (code > 0)
                            {
                                double num1 = result / code;
                                temp1[0]["转化率"] = Math.Round(num1 * 100, 2);

                            }

                        }
                        else
                        {

                            DataRow dRow = dt.NewRow();
                            dRow["公司名称"] = dt1.Rows[i]["name"].ToString();
                            dRow["通道"] = dt1.Rows[i]["names"].ToString();
                            dRow["产品名称"] = dt1.Rows[i]["产品名称"].ToString();
                            dRow["上行总量"] = Convert.ToInt32(dt1.Rows[i]["上行总量"]);
                            dRow["验证码总量"] = Convert.ToInt32(dt1.Rows[i]["验证码总量"]);
                            dRow["独立验证码"] = Convert.ToInt32(dt1.Rows[i]["独立验证码"]);
                            dRow["资费"] = Convert.ToSingle(dt1.Rows[i]["fee"]);

                            dt.Rows.Add(dRow);
                        }
                    }
                }
                dt.DefaultView.Sort = "公司名称 ASC";
                dt = dt.DefaultView.ToTable();

            }
            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";


            return info;
        }



        public static string getMultiServiceDataxx(string search, string node)
        {
            string info = null;
            string sql = null;
            string sql1 = null;
            string hours=null;
            string day = null;
            string param = "";
            string param1 = "";
            string param2 = "";
            JObject datajson = JObject.Parse(search);
            DataTable dt = new DataTable();

            if (!string.IsNullOrEmpty(datajson["sdate"].ToString()) && !string.IsNullOrEmpty(datajson["edate"].ToString()))
            {

                string eday = datajson["edate"].ToString();
                string[] temp = eday.Split('-');
                if (temp[1].Length < 2)
                    temp[1] = "0" + temp[1];
                if (temp[2].Length < 2)
                    temp[2] = "0" + temp[2];
                datajson["edate"] = temp[0] + "-" + temp[1] + "-" + temp[2];
                DateTime t = Convert.ToDateTime(datajson["edate"].ToString());
                DateTime t1 = t.AddDays(1);
                day = t1.ToString("yyyy-MM-dd");
                hours = " and (b.datatime>='" + t.AddDays(-3).ToString("yyyy-MM-dd") + "' and b.datatime<'" + day + "')";
                param += "(a.datatime>='" + datajson["sdate"].ToString() + "' and a.datatime<'" + day + "')";
                param2 = param;

            }

            /*if (!string.IsNullOrEmpty(datajson["conduit"].ToString()))
            {
                if (Convert.ToInt32(datajson["conduit"]) > 0)
                {
                    param += " and a.conduitID=" + datajson["conduit"].ToString();
                    param1 += " and a.conduitID=" + datajson["conduit"].ToString();
                    param2 += " and a.conduitID=" + datajson["conduit"].ToString();
                }
            }

            if (!string.IsNullOrEmpty(datajson["company"].ToString()))
            {
                if (Convert.ToInt32(datajson["company"]) > 0)
                {
                    param += " and a.companyID=" + datajson["company"].ToString();
                    param1 += " and a.companyID=" + datajson["company"].ToString();
                }
            }

            if (!string.IsNullOrEmpty(datajson["product"].ToString()))
            {
                if (Convert.ToInt32(datajson["product"]) > 0)
                {
                    param += " and a.productID=" + datajson["product"].ToString();
                    param1 += " and a.productID=" + datajson["product"].ToString();
                    param2 += " and a.productID=" + datajson["product"].ToString();
                }
            }*/

            if (!string.IsNullOrEmpty(node))
            {
                JObject json = JObject.Parse(node);
                if (json["level"].ToString() == "0")
                {
                    param += " and a.conduitID in (" + json["list"].ToString() + ")";
                    param1 += " and a.conduitID in (" + json["list"].ToString() + ")";
                    param2 += " and a.conduitID in (" + json["list"].ToString() + ")";
                }
                else if (json["level"].ToString() == "1")
                {
                    param += " and a.productID=" + json["infoid"].ToString();
                    param1 += " and a.productID in (" + json["infoid"].ToString() + ")";
                    param2 += " and a.productID in (" + json["infoid"].ToString() + ")";
                }
                else if (json["level"].ToString() == "2")
                {
                    param += " and a.productID=" + json["pid"].ToString() + " and a.companyID=" + json["infoid"].ToString();
                    param1 += " and a.productID=" + json["pid"].ToString() + " and a.companyID=" + json["infoid"].ToString();
                    param2 += " and a.productID=" + json["pid"].ToString() + " and a.companyID=" + json["infoid"].ToString();
                }
            }

            if (Convert.ToInt32(datajson["index"]) == 1)
            {

                sql = "WITH x1 AS (select "
                     + "convert(char(10),a.datatime,120) as datatime,"
                     + "sum(case when a.codeflag=0 then 1 else 0 end) as upstream,"
                     + "sum(case when a.codeflag=1 then 1 else 0 end) as verifycode,"
                     + "COUNT(DISTINCT(case when buyid=2 and len(IMSI)>0 and codeflag=1 then IMSI when buyid=2 and len(mobile)>0 and codeflag=1 then mobile end))+COUNT(DISTINCT(case when buyid<>2 and len(IMSI)>0 and codeflag=1 then IMSI when buyid<>2 and len(mobile)>0 and codeflag=1 then mobile end)) as singlecode "
                     + "from public_order_2017 as a WITH(NOLOCK) "
                     + "where " + param
                     + "group by convert(char(10),a.datatime,120)),"
                   + "x2 AS (select "
                     + "convert(char(10),a.datatime,120) as datatime,"
                     + "sum(case when a.optype<>1 then 1 else 0 end) as notifyall,"
                     + "sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end) as orderall,"
                     //+ "sum(case when a.resultCode=0 and a.optype=0 then case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end else 0 end) as amount,"
                     + "sum(case when a.optype=1 then 1 end) as cancelall,"
                     + "sum(case when c.resultCode=0 and c.optype=1 then 1 else 0 end) as synccancel,"
                     + "sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end) as syncorders,"
                     + "case when sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end))/ Convert(decimal(18,2),sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))*100) else 0 end as syncrate,"
                     + "sum(case when c.resultCode=0 and c.result=0 and c.optype=0 then case when len(c.fee)>2 then Convert(decimal(18,2),c.fee)/100 else Convert(decimal(18,2),c.fee) end else 0 end) as syncamount,"
                     + "sum(case when c.result=1 and c.optype=0 then 1 else 0 end) as result "
                     + "from public_notify_2017 as a WITH(NOLOCK) "
                     + "left join public_sync_2017 as c WITH(NOLOCK) on c.infoid=a.infoid "
                     + "where " + param
                     + "group by convert(char(10),a.datatime,120)) "
                     + "select case when len(x1.datatime)>0 then x1.datatime else x2.datatime end  as 日期,x1.upstream as 上行总量,x1.verifycode as 验证码总量,x1.singlecode as 独立验证码,"
                     + "(select max(id) from(select ROW_NUMBER() OVER(ORDER BY a.userOrder,a.mobile) as id from public_notify_2017 as a WITH(NOLOCK) where " + param + " and a.OPType=1 and a.hours72=1 group by a.userOrder,a.mobile) as tab) as hours,"
                     + "case when x2.orderall>0 and x1.singlecode>0 then Convert(decimal(18,2),Convert(decimal(18,2),x2.orderall)/Convert(decimal(18,2),x1.singlecode)*100) when x1.upstream>0 and x1.singlecode=0 then Convert(decimal(18,2),Convert(decimal(18,2),x2.orderall)/Convert(decimal(18,2),x1.upstream)*100) else 0 end as 转化率,"
                     + "x2.notifyall as 通知订购总量,x2.orderall as 成功订购总量,x2.synccancel as 同步退订总量,x2.syncorders as 同步成功总量,x2.cancelall as 退订总量,"
                     + "(select max(id) from(select ROW_NUMBER() OVER(ORDER BY userOrder,mobile) as id from public_notify_2017 WITH(NOLOCK) where  " + param.Replace("a.","") + " and OPType=0 and resultCode=0 group by userOrder,mobile) as tab) as 独立用户,"
                     + "(select sum(Convert(decimal(18,2),fee)) from(select ROW_NUMBER() OVER(ORDER BY userOrder,mobile,fee) as id,(case when len(fee)>2 then Convert(decimal(18,2),fee)/100 else Convert(decimal(18,2),fee) end) as fee from public_notify_2017 WITH(NOLOCK) where " + param.Replace("a.", "") + " and OPType=0 and resultCode=0 group by userOrder,mobile,fee) as tab)  as 信息费,"
                     //+ "case when x2.syncorders>0 then Convert(decimal(18,2),Convert(decimal(18,2),x2.syncorders)/x2.orderall)*100 else 0 end as 同步率,"
                     + "x2.syncrate as 同步率,x2.syncamount as 同步信息费,x2.result as 回掉失败 from x1 full join x2 on x1.datatime=x2.datatime";

                     dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            }
            else if (Convert.ToInt32(datajson["index"]) == 2)
            {
                sql = "WITH x1 AS (select "
                         +"case when CHARINDEX(' ',a.area)=0 then a.area else SUBSTRING(a.area,0,CHARINDEX(' ',a.area)) end as area,"
                         +"sum(case when a.codeflag=0 then 1 else 0 end) as upstream,"
                         +"sum(case when a.codeflag=1 then 1 else 0 end) as verifycode,"
                         + "COUNT(DISTINCT(case when buyid=2 and len(IMSI)>0 and codeflag=1 then IMSI when buyid=2 and len(mobile)>0 and codeflag=1 then mobile end))+COUNT(DISTINCT(case when buyid<>2 and len(IMSI)>0 and codeflag=1 then IMSI when buyid<>2 and len(mobile)>0 and codeflag=1 then mobile end)) as singlecode "
                         +"from public_order_2017 as a WITH(NOLOCK) "
                         +"where "+param
                         + "group by case when CHARINDEX(' ',a.area)=0 then a.area else SUBSTRING(a.area,0,CHARINDEX(' ',a.area)) end),"
                      +"x2 AS (select "
                         +"case when CHARINDEX(' ',a.area)=0 then a.area else SUBSTRING(a.area,0,CHARINDEX(' ',a.area)) end as area,"
                         +"sum(case when a.optype<>1 then 1 else 0 end) as notifyall,"
	                     +"sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end) as orderall,"
	                     //+"sum(case when a.resultCode=0 and a.optype=0 then case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end else 0 end) as amount,"
                         +"sum(case when a.optype=1 then 1 end) as cancelall,"
                         +"sum(case when c.resultCode=0 and c.optype=1 then 1 else 0 end) as synccancel,"
                         +"sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end) as syncorders,"
                         +"case when sum(case when c.resultCode=0 and a.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),"
                         +"sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end))/Convert(decimal(18,2),"
                         +"sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))*100) else 0 end as syncrate,"
                         +"sum(case when c.resultCode=0 and c.result=0 and c.optype=0 then case when len(c.fee)>2 then Convert(decimal(18,2),c.fee)/100 else Convert(decimal(18,2),c.fee) end else 0 end) as syncamount,"
                         +"sum(case when c.result=1 and c.optype=0 then 1 else 0 end) as result "
                         +"from public_notify_2017 as a WITH(NOLOCK) "
                         +"left join public_sync_2017 as c WITH(NOLOCK) on c.infoid=a.infoid "
                         + "where " + param
                         + "group by case when CHARINDEX(' ',a.area)=0 then a.area else SUBSTRING(a.area,0,CHARINDEX(' ',a.area)) end) "
                    + "select b.name as 省份, upstream as 上行总量,verifycode as 验证码总量,singlecode as 独立验证码,notifyall as 通知订购总量,orderall as 成功订购总量,orderall as 成功订购总量,"
                         + "case when orderall>0 and singlecode>0 then Convert(decimal(18,2),Convert(decimal(18,2),orderall)/Convert(decimal(18,2),singlecode)*100) when upstream>0 and singlecode=0 then Convert(decimal(18,2),Convert(decimal(18,2),orderall)/Convert(decimal(18,2),upstream)*100) else 0 end as 转化率,"
                         + "cancelall as 退订总量,synccancel as 同步退订总量,syncorders as 同步成功总量,syncrate as 同步率,"
                         + "syncamount as 同步信息费,result as 回掉失败,"
                         + "(select max(id) from(select ROW_NUMBER() OVER(ORDER BY userOrder,mobile) as id from public_notify_2017 WITH(NOLOCK) where " + param.Replace("a.", "") + " and OPType=1 and hours72=1 and CHARINDEX(b.name,area)>0 group by userOrder,mobile) as tab) as hours,"
                         + "(select max(id) from(select ROW_NUMBER() OVER(ORDER BY userOrder,mobile) as id from public_notify_2017 WITH(NOLOCK) where " + param.Replace("a.", "") + " and OPType=0 and resultCode=0 and CHARINDEX(b.name,area)>0 group by userOrder,mobile) as tab) as 独立用户,"
                         + "(select sum(Convert(decimal(18,2),fee)) from(select ROW_NUMBER() OVER(ORDER BY userOrder,mobile,fee) as id,(case when len(fee)>2 then Convert(decimal(18,2),fee)/100 else Convert(decimal(18,2),fee) end) as fee from public_notify_2017 WITH(NOLOCK) where " + param.Replace("a.", "") + " and OPType=0 and resultCode=0 and CHARINDEX(b.name,area)>0 group by userOrder,mobile,fee) as tab)  as 信息费 "
                         + "from(select case when len(x1.area)>0 then x1.area else x2.area end as area,x1.upstream,x1.verifycode,x1.singlecode,x2.cancelall,"
                         + "x2.notifyall,x2.orderall,x2.synccancel,x2.syncorders,x2.syncrate,x2.syncamount,x2.result "
                         + "from x1 Full join x2 on x2.area=x1.area) as tabs Full join areaInfo as b on b.name=tabs.area where b.parent_id=0 order by b.id";

                         dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            }
            else
            {
                sql = "WITH x1 AS (select "
                         + "a.conduitID,a.productID,a.companyID,"
	                     + "case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end as fee,"
                         + "sum(case when a.codeflag=0 then 1 else 0 end) as upstream,"
                         + "sum(case when a.codeflag=1 then 1 else 0 end) as verifycode,"
                         + "COUNT(DISTINCT(case when buyid=2 and len(IMSI)>0 and codeflag=1 then IMSI when buyid=2 and len(mobile)>0 and codeflag=1 then mobile end))+COUNT(DISTINCT(case when buyid<>2 and len(IMSI)>0 and codeflag=1 then IMSI when buyid<>2 and len(mobile)>0 and codeflag=1 then mobile end)) as singlecode "
                         + "from public_order_2017 as a WITH(NOLOCK) "
                         + "where "+ param
                         + "group by a.conduitID,a.productID,a.companyID,a.fee),"
                      + "x2 AS (select "
                         + "a.conduitID,a.productID,a.companyID,"
	                     + "case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end as fee,"
	                     + "sum(case when a.optype=1 then 1 end) as cancelall,"
	                     + "COUNT(DISTINCT(case when a.hours72=1 then case when len(a.mobile)>0 then a.mobile else a.userOrder end end)) as hours72,"
                         + "COUNT(DISTINCT(case when a.resultCode=0 and a.optype=0 then (case when len(a.userOrder)>0 then a.userOrder else a.mobile end) end)) as singleuser,"
                         + "sum(case when a.optype<>1 then 1 else 0 end) as notifyall,"
	                     + "sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end) as orderall,"
	                     //+ "sum(case when a.resultCode=0 and a.optype=0 then case when len(a.fee)>2 then Convert(decimal(18,2),a.fee)/100 else Convert(decimal(18,2),a.fee) end else 0 end) as amount,"
                         + "sum(case when c.resultCode=0 and c.optype=1 then 1 else 0 end) as synccancel,"
                         + "sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end) as syncorders,"
                         + "case when sum(case when c.resultCode=0 and a.optype=0 then 1 else 0 end)>0 then Convert(decimal(18,2),Convert(decimal(18,2),"
                         + "sum(case when c.resultCode=0 and c.optype=0 then 1 else 0 end))/Convert(decimal(18,2),"
                         + "sum(case when a.resultCode=0 and a.optype=0 then 1 else 0 end))*100) else 0 end as syncrate,"
                         + "sum(case when c.resultCode=0 and c.result=0 and c.optype=0 then case when len(c.fee)>2 then Convert(decimal(18,2),c.fee)/100 else Convert(decimal(18,2),c.fee) end else 0 end) as syncamount,"
                         + "sum(case when c.result=1 and c.optype=0 then 1 else 0 end) as result "
                         + "from public_notify_2017 as a WITH(NOLOCK) "
                         + "left join public_sync_2017 as c WITH(NOLOCK) on c.infoid=a.infoid "
                         + "where "+param
                         + "group by a.conduitID,a.productID,a.companyID,a.fee) "
                      + "select e.names as 通道,d.name as 公司名称,fee as 资费,(select name from product where infoid=productID) as 产品名称,upstream as 上行总量,"
                         + "verifycode as 验证码总量,singlecode as 独立验证码,notifyall as 通知订购总量,orderall as 成功订购总量,singleuser as 独立用户,"
                         + "case when orderall>0 and singlecode>0 then Convert(decimal(18,2),Convert(decimal(18,2),orderall)/Convert(decimal(18,2),singlecode)*100) when upstream>0 and singlecode=0 then Convert(decimal(18,2),Convert(decimal(18,2),orderall)/Convert(decimal(18,2),upstream)*100) else 0 end as 转化率,"
                         + "cancelall as 退订总量,hours72 as hours,synccancel as 同步退订总量,syncorders as 同步成功总量,syncrate as 同步率,"
                         + "syncamount as 同步信息费,result as 回掉失败,"
                         +"(Convert(decimal(18,2),singleuser*fee)) as 信息费 "
                         + "from(select x1.upstream,x1.verifycode,x1.singlecode,"
                         + "x2.notifyall,x2.orderall,x2.singleuser,x2.cancelall,x2.hours72,x2.synccancel,x2.syncorders,x2.syncrate,x2.syncamount,x2.result,"
	                     + "case when len(x1.companyID)>0 then x1.companyID else x2.companyID end as companyID,"
                         + "case when len(x1.productID)>0 then x1.productID else x2.productID end as productID,"
                         + "case when len(x1.conduitID)>0 then x1.conduitID else x2.conduitID end as conduitID,"
                         + "case when len(x1.fee)>0 then x1.fee else x2.fee end as fee "
                         + "from x1 Full join x2 on x2.conduitID=x1.conduitID and x2.companyID=x1.companyID and x2.productID=x1.productID and x2.fee=x1.fee) as tab "
                         + "left join conduit as e on e.infoid=tab.conduitid "
                         + "left join company as d on d.infoid=tab.companyid order by d.name";

                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            }
            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";


            return info;
        }


        /// <summary>
        /// 获取配置信息
        /// </summary>
        /// <param name="conduitid"></param>
        /// <param name="step"></param>
        /// <returns></returns>
        public static DataTable getConfigInfo(int conduitid, int step,int groupid)
        {
            DataTable dt=new DataTable();

            string sql = "select * from public_Config where conduitid=" + conduitid + " and groupid=" + groupid;
            if (step > 0)
                sql += " and step=" + step;

            dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            return dt;
        }

        /// <summary>
        /// 获取同步接口
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static DataTable getSyncFace(int infoid)
        {
            string sql = "select b.outField,b.inField,b.productid,b.companyid,b.actionid,b.sort from public_interface as b where b.configid=" + infoid;
            DataTable field = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            return field;
        }

        public static DataTable getSyncFace(int conduitid,int groupid)
        {
            string sql = "select b.outField,b.inField,b.productid,b.companyid,b.actionid,b.sort from public_config as a left join public_interface as b on b.configid=a.infoid where a.conduitid=" + conduitid + " and a.step=4 and a.groupid=" + groupid;
            DataTable field = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            return field;
        }

        /// <summary>
        /// 获取同步表插入字段
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static DataTable getSyncTableField(int infoid)
        {
            
            DataTable dt = new DataTable();
            try
            {
                string sql = "select b.fieldname from public_field as b where b.configid=" + infoid;
                dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
               
            }
            catch (Exception e)
            {
               
                return dt;
            }

            return dt;

        }

        /// <summary>
        /// 获取json值
        /// </summary>
        /// <param name="json"></param>
        /// <param name="node"></param>
        /// <returns></returns>
        public static string FindJsonNodex(string json, string node)
        {
            string data = string.Empty;
            int spoint = json.IndexOf(node);
            if (spoint < 0)
                return null;
            string str = json.Substring(spoint);
            int epoint = str.IndexOf(",");
            if (epoint == -1)
                data = json.Substring(spoint, (str.Length - 1));
            else
                data = json.Substring(spoint, epoint);

            return data.Substring(data.IndexOf(":") + 1).Replace("\"", "").Trim();
        }


       /// <summary>
       /// 设置同步接口数据
       /// </summary>
       /// <param name="fromatid"></param>
       /// <param name="data"></param>
       /// <param name="fromatCode"></param>
       /// <param name="valuename"></param>
       /// <returns></returns>
       public static string setFormatInterfaceData(int fromatid, DataTable data, string fromatCode, DataTable face)
        {
                string param = string.Empty;
                face.DefaultView.Sort = "sort ASC";
                face = face.DefaultView.ToTable();
                face.Columns.Remove("sort");

                if (fromatid == 2)
                {
                    foreach (DataRow dr in face.Rows)
                    {
                            string field = dr["inField"].ToString();
                            if(!string.IsNullOrEmpty(field))
                            {
                               if (null != data.Rows[0][field])
                                  param += "&" + dr["outField"].ToString() + "=" + data.Rows[0][field].ToString();
                            }
                            else
                               param += "&" + dr["outField"].ToString() + "=";
                    
                    }
                    param = param.Substring(1);
                }
                else if (fromatid == 1 || fromatid == 0)
                {
                    if (!string.IsNullOrEmpty(fromatCode))
                    {
                        param = fromatCode;
                        foreach (DataRow dr in face.Rows)
                        {
                                string field = dr["inField"].ToString();
                                if (!string.IsNullOrEmpty(field))
                                {
                                   if (null != data.Rows[0][field])
                                      param = FindJsonNodex(param, dr["outField"].ToString(), data.Rows[0][field].ToString());
                                }
                                else
                                   param= FindJsonNodex(param, dr["outField"].ToString(), "");
                        }
                    }
                    else
                    {
                        foreach (DataRow dr in face.Rows)
                        {
                                string field = dr["inField"].ToString();
                                if (!string.IsNullOrEmpty(field))
                                {
                                    if (null != data.Rows[0][field])
                                        param += ",\"" + dr["outField"].ToString() + "\":\"" + data.Rows[0][field].ToString() + "\"";
                                }
                                else
                                   param += ",\"" + dr["outField"].ToString() + "\":\"\"";
                        }

                        param = "{" + param.Substring(1) + "}";
                    }
                }
            
            return param;
        }


        public static void RunSyncData(int infoid,string key)
        {
            
             int number=0;
             int sync = 0;
             string items="syncpack";
             string value=string.Empty;

             
                 string sql = "select a.* ,b.syncUrl,b.syncMethod,b.groupid from public_notify_2017 as a WITH(NOLOCK) left join action as b on b.companyid=a.companyid and b.productcode=a.productcode where a.infoid=" + infoid;

                 DataTable data = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

                 if (data.Rows.Count > 0)
                 {
                     DataTable config = getConfigInfo(Convert.ToInt32(data.Rows[0]["conduitid"]), 4, Convert.ToInt32(data.Rows[0]["groupid"]));
                     if (config.Rows.Count > 0)
                     {
                         /*DataTable syncdata = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "select top 1 infoid form " + config.Rows[0]["tablename"].ToString() + " WITH(NOLOCK) where infoid=" + infoid).Tables[0];
                         if (syncdata.Rows.Count == 0)
                         {*/
                             DataTable face = getSyncFace(Convert.ToInt32(config.Rows[0]["infoid"]));
                             if (face.Rows.Count > 0)
                             {
                                 DataTable field = getSyncTableField(Convert.ToInt32(config.Rows[0]["infoid"]));
                                 if (field.Rows.Count > 0)
                                 {
                                     string param = setFormatInterfaceData(Convert.ToInt32(config.Rows[0]["syncFormat"]), data, config.Rows[0]["syncCode"].ToString(), face);
                                     value = "'" + data.Rows[0]["syncUrl"].ToString() + "?" + param + "'";

                                     foreach (DataRow dr in field.Rows)
                                     {
                                         string name = dr["fieldname"].ToString();
                                         items += "," + name;
                                         if (name == "datatime")
                                         {
                                             DateTime daytime = Convert.ToDateTime(data.Rows[0][name]);
                                             value += ",'" + daytime.ToShortDateString() + " " + daytime.TimeOfDay.ToString() + "'";
                                         }
                                         else
                                             value += ",'" + data.Rows[0][name].ToString() + "'";
                                     }
                                     string insert = "INSERT INTO " + config.Rows[0]["tablename"].ToString() + "(" + items + ") values(" + value + ")";

                                     number = SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, insert);

                                     if (number > 0)
                                     {
                                        string info = Utils.GetService(data.Rows[0]["syncMethod"].ToString(), data.Rows[0]["syncUrl"].ToString(), param, 0);
                                        if (info == "error")
                                            sync = updateSyncData(infoid, 1);
                                     }
                                     cacheJson(key, number, sync);
                                 }
                             }
                         //}
                     }
                 }
        }

        static void cacheJson(string key,int number,int sync)
        {
            DefaultCacheStrategy cache = new DefaultCacheStrategy();
            object info=cache.RetrieveObject(key);
            if (null != info)
            {
                JObject json =info as JObject;
                if (number > 0)
                    json["number"] = Convert.ToInt32(json["number"]) + 1;
                if(sync>0)
                    json["sync"] = Convert.ToInt32(json["sync"]) + 1;

                cache.RemoveObject(key);
                cache.AddObjectWith(key, json);
            }
          
        }

        public static void restRrunSyncData(string key,int infoid)
        {
            int id = 0;
            string sql = "select a.syncpack,b.syncUrl,b.syncMethod from public_sync_2017 as a WITH(NOLOCK) left join action as b on b.companyid=a.companyid and b.productcode=a.productcode where a.infoid=" + infoid;
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            if (dt.Rows.Count > 0)
            {
                 string[] pack=dt.Rows[0]["syncpack"].ToString().Split('?');
                 string info = Utils.GetService(dt.Rows[0]["syncMethod"].ToString(), dt.Rows[0]["syncUrl"].ToString(), pack[1], 0);
                 if (info == "error")
                 {
                     id = updateSyncData(infoid, 1);
                     cacheJson(key, 0, id);
                 }
                 else
                     updateSyncData(infoid, 0);
            }
        }

        static int updateSyncData(int infoid,int flag)
        {
            string sql = "update public_sync_2017 set result="+flag+" where infoid=" + infoid;
            return SqlHelper.ExecuteNonQuery(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql);  
        }


        /// <summary>
        /// 获取规则数据
        /// </summary>
        /// <param name="productid">产品id</param>
        /// <param name="companyid"></param>
        /// <param name="actioinid"></param>
        /// <returns></returns>
        public static DataTable getRulesInfo(int productid, int companyid, int actioinid)
        {
            
            string sql = string.Format("select infoid,limit,productid,companyid,actionid,typeid,sort from rules as a WITH(NOLOCK) where productid={0} and (companyid=0 or actionid=0) order by typeid", productid);
            if (companyid>0)
                sql = string.Format("select infoid,limit,productid,companyid,actionid,typeid,sort from rules as a WITH(NOLOCK) where productid={0} and companyid={1} order by typeid", productid, companyid);
            if (actioinid>0)
                sql = string.Format("select infoid,limit,productid,companyid,actionid,typeid,sort from rules as a WITH(NOLOCK) where productid={0} and actioinid={1} order by typeid", productid, actioinid);
            
           return SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
           
        }


        public static DataTable getRulesInfo(int infoid)
        {
            string sql = string.Format("select infoid,limit,productid,companyid,actionid,typeid,sort from rules as a WITH(NOLOCK) where infoid={0}", infoid);
        
            return SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

        }

        /// <summary>
        /// 获取无收入数据
        /// </summary>
        /// <returns></returns>
        public static DataTable getNoIncome(string idlist)
        {
            string param=string.Empty;
            if (!string.IsNullOrEmpty(idlist))
                param = " and productid in(" + idlist + ")";
            string sql = "select productid,companyID,updata,(select name from product where infoid=productid) as productname,(select name from company where infoid=companyID) as cname from(select a.productid,b.companyID,(select count(datatime) from public_order_2017 WITH(NOLOCK) where datatime>CONVERT(varchar(10),GETDATE(),120) and productid=a.productid and companyid=b.companyID and codeflag=0) as updata,(select top 1 count(infoid) from public_notify_2017 WITH(NOLOCK) where datatime>CONVERT(varchar(10),GETDATE(),120) and productid=a.productid and companyid=b.companyID and resultCode=0) as infoid from public_statistics as a left join action as b on b.productID=a.productid where datatime>CONVERT(varchar(10),dateadd(day,-CAST(DATEPART(dd,GETDATE()) AS int)+1,dateadd(month,-1,GETDATE())),120) and datatime<CONVERT(varchar(10),dateadd(day,1,GETDATE()),120) and b.companyID>0 and b.companyID<>2 group by a.productid,b.companyID) as tab where infoid=0 " + param + " order by productid";
            return SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
        }



        /// <summary>
        /// 获取票据信息
        /// </summary>
        /// <param name="field"></param>
        /// <param name="companyid"></param>
        /// <param name="flag"></param>
        /// <returns></returns>
        public static string getCPPropertyInfo(string field, int companyid, int flag)
        {
            string info ="{data:[]}";
            string sql = string.Format("select * from CPPropertyInfo where companyid={0} and flag={1}", companyid, flag); ;
            if (!string.IsNullOrEmpty(field))
                sql = string.Format("select {0} from CPPropertyInfo where companyid={1} and flag={2}", field, companyid, flag);

            DataTable dt=SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
            {
                info = JsonConvert.SerializeObject(dt, new DataTableConverter());
                info = info.Replace("\"[", "[").Replace("]\"", "]").Replace("\\", "");
            }

            return info;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="pagesize"></param>
        /// <param name="currentpage"></param>
        /// <param name="search"></param>
        /// <returns></returns>
        public static string getReceivables(int pagesize, int currentpage, string search)
        {
            string info="{data:[]}";
            string pid = "";
            int TotalRecords = 0;
            string m_param = null;
            string w_param = null;
            string d_param = null;
            string p_param = "";
            string sqlParam= "";
            Time tm = new Time();

            if(string.IsNullOrEmpty(search))
                return info;

            DateTime pmonthday = new DateTime(DateTime.Now.Year, (DateTime.Now.Month - 1), 1).AddMonths(1).AddDays(-1);//上月最后一天日期
            DateTime pyearday = Convert.ToDateTime((DateTime.Now.Year - 1).ToString() + "-01-01");//上一年第一天日期
            DateTime firstday = tm.GetWeekFirstDayMon(pyearday);//上一年第一天所在周的周一日期
            int pwday = tm.GetDateWeek(firstday);//上一年第一天所在周的周一是星期几
            int wday = tm.GetDateWeek(DateTime.Now);//今天是星期几
            m_param = " or (a.datatime between '" + (DateTime.Now.Year - 1).ToString() + "-01-01' and '" + pmonthday.ToShortDateString() + "' and settlement=0)";//月结

            if (pwday == 1)
               firstday = tm.GetWeekFirstDayMon(pyearday.AddDays(-1));

            w_param = " or (a.datatime between '" + firstday.ToShortDateString() + "' and '" +  DateTime.Now.AddDays(-wday).ToShortDateString() + "' and settlement=1)";//周结

            d_param = " or (a.datatime between '" + pyearday.AddDays(1).ToShortDateString() + "' and '" + DateTime.Now.AddDays(-1).ToShortDateString() + "' and settlement=2)";//日结

            
            JObject json = JObject.Parse(search);
            if (null != json["date"])
                {
                    
                    m_param ="";
                    w_param ="";
                    d_param ="";
                   
                    string[] list = json["date"].ToString().Split(',');
                    for (int i = 0; i < list.Length; i++)
                    {
                        DateTime lastday = tm.LastDayOfMonth(Convert.ToDateTime(list[i]));//所在月的最后一天
                        DateTime wfm = tm.GetWeekFirstDayMon(Convert.ToDateTime(list[i]));//所在周的第一天
                        int fwday = tm.GetDateWeek(wfm);//所在周的第一天是星期几
                        int wd = tm.GetDateWeek(lastday);//所在月的最后一天是星期几
                        if (fwday==1)
                            wfm = tm.GetWeekFirstDayMon(Convert.ToDateTime(list[i]).AddDays(-1));

                        if (list[i].IndexOf(DateTime.Now.ToString("yyyy-MM")) > -1)//判断是否本月
                        {
                            m_param += " or (a.datatime between '" + tm.FirstDayOfPreviousMonth(Convert.ToDateTime(list[i])).ToShortDateString() + "' and '" + tm.LastDayOfPrdviousMonth(Convert.ToDateTime(list[i])) + "' and settlement=0)";
                            w_param += " or (a.datatime between '" + wfm.ToShortDateString() + "' and '" + DateTime.Now.AddDays(-wday).ToShortDateString() + "' and settlement=1)";
                            d_param += " or (a.datatime between '" + Convert.ToDateTime(list[i]).AddDays(1).ToShortDateString() + "' and '" + DateTime.Now.AddDays(-1).ToShortDateString() + "' and settlement=2)";
                        }
                        else
                        {
                            m_param += " or (a.datatime between '" + list[i] + "' and '" + lastday.ToShortDateString() + "' and settlement=0)";
                            w_param += " or (a.datatime between '" + wfm.ToShortDateString() + "' and '" + lastday.AddDays(-wd).ToShortDateString() + "' and settlement=1)";
                            d_param += " or (a.datatime between '" + Convert.ToDateTime(list[i]).AddDays(1).ToShortDateString() + "' and '" + lastday.AddDays(-1).ToShortDateString() + "' and settlement=2)";
                        }
                    }
                }

            string msql = "select (select case when pid>0 then pid else infoid end from company where infoid=a.companyID) as pid,"
                + "a.companyID,a.productID,a.conduitID,syncamount,divideInto,settlement,"
                + "convert(varchar(7),a.datatime,23) as times "
                + "from public_statistics as a WITH(NOLOCK) left join action as c on c.productid=a.productid and c.companyid=a.companyid "
                + "where (" + m_param.Substring(4) + ") and a.pflag=0 and a.syncamount>0 and a.companyid<>2";

            string wsql = "select (select case when pid>0 then pid else infoid end from company where infoid=a.companyID) as pid,"
                + "a.companyID,a.productID,a.conduitID,syncamount,divideInto,settlement,"
                + "(case when datepart(weekday,a.datatime)=1 then convert(varchar(100),dateadd(dd,-6,a.datatime),23) else convert(varchar(100),DATEADD(wk, DATEDIFF(wk,0,a.datatime), 0),23) end+'~'+case when datepart(weekday,a.datatime)=1 then convert(varchar(100),a.datatime,23) else convert(varchar(100),DATEADD(wk, DATEDIFF(wk,0,a.datatime), 6),23) end) as times "
                + "from public_statistics as a WITH(NOLOCK) left join action as c on c.productid=a.productid and c.companyid=a.companyid "
                + "where (" + w_param.Substring(4) + ") and a.pflag=0 and a.syncamount>0 and a.companyid<>2";

            string dsql = "select (select case when pid>0 then pid else infoid end from company where infoid=a.companyID) as pid,"
                + "a.companyID,a.productID,a.conduitID,syncamount,divideInto,settlement,"
                + "convert(varchar(10),a.datatime,23) as times "
                + "from public_statistics as a WITH(NOLOCK) left join action as c on c.productid=a.productid and c.companyid=a.companyid "
                + "where (" + d_param.Substring(4) + ") and a.pflag=0 and a.syncamount>0 and a.companyid<>2";


            if (json["settlement"].ToString().IndexOf("2") > -1)
            {
                sqlParam = dsql;
                p_param += d_param;
            }
            else
            {
                if (json["settlement"].ToString().IndexOf("0,1") > -1)
                {
                    sqlParam += msql + " UNION ALL " + wsql;
                    p_param += m_param + w_param;
                }
                else if (json["settlement"].ToString().IndexOf("0") > -1)
                {
                    sqlParam += msql;
                    p_param += m_param;
                }
                else if(json["settlement"].ToString().IndexOf("1") > -1)
                {
                    sqlParam += wsql;
                    p_param += w_param;
                }
                else if (json["settlement"].ToString().IndexOf("2") > -1)
                {
                    sqlParam = dsql;
                    p_param = d_param;
                }
            }

            if (null != json["company"])
            {
                DataTable cpid = getCompanyInfo(json["company"].ToString());
                if (cpid.Rows.Count > 0)
                {
                    string ls="";
                    for (int i = 0; i < cpid.Rows.Count; i++)
                        ls += "," + cpid.Rows[i]["infoid"].ToString();

                    p_param += " and a.companyid in(" + ls.Substring(1) + ")";
                }
            }
               
                   string sqlp = "select companyid from(select x.companyid,"
                      + "(select top 1 pid from company where infoid=x.companyid) as pid "
                      + "from public_statistics as x left join action as c on c.productid=x.productid and c.companyid=x.companyid "
                      + "where (" + p_param.Substring(4).Replace("a.", "x.") + ") and x.pflag=0 and x.syncamount>0 and x.companyid<>2 "
                      + "group by x.companyid,x.productid) as cp "
                      + "where pid=0 group by companyid order by companyid";
                  
                    DataTable idlist = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sqlp).Tables[0];
                    if (idlist.Rows.Count > 0)
                      {
                              TotalRecords = idlist.Rows.Count;
                              int i = (pagesize * currentpage) - pagesize;
                              int n = (pagesize * currentpage);
                              int pagemax = 0;

                              if (idlist.Rows.Count % pagesize == 0)
                                  pagemax = idlist.Rows.Count / pagesize;
                              else
                                  pagemax = idlist.Rows.Count / pagesize + 1;
                              
                              if (null != json["company"])
                              {
                                   string id="";
                                   while (i < n && i < idlist.Rows.Count)
                                   {
                                        id += "," + idlist.Rows[i][0].ToString();
                                        ++i;
                                   }
                                   pid="pid in("+id.Substring(1)+")";
                              }
                              else
                              {
                                  if (pagemax<=1)
                                      pid = "pid>0";
                                  else if (currentpage < pagemax)
                                      pid = "pid>=" + idlist.Rows[i][0].ToString() + " and pid<" + idlist.Rows[n][0].ToString();
                                  else
                                      pid = "pid>=" + idlist.Rows[i][0].ToString();
                              }

                              string sql = string.Format("select (select name from company where infoid=tab.pid) pname,"
                                + "(select name from company where infoid=tab.companyid) name,"
                                + "(select names from conduit where infoid=tab.conduitid) as 通道,"
                                + "(select name from product where infoid=tab.productid) as 产品名称,"
                                + "tab.times,tab.pid,tab.companyID,tab.productID,tab.conduitID,tab.settlement,tab.divideInto,sum(tab.syncamount) 信息费,"
                                + "Convert(decimal(18,2),(Convert(decimal(18,2),tab.divideInto)/100)*sum(tab.syncamount)) as 结算金额 from( "
                                + "{0}) as tab "
                                + "where {1} "
                                + "group by tab.companyID,tab.productID,tab.conduitID,tab.settlement,tab.divideInto,tab.times,tab.pid "
                                + "order by tab.pid asc,tab.productid,tab.times desc", sqlParam,pid);

                               DataTable dt=SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

                               if (dt.Rows.Count > 0)
                                  info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",total:" + TotalRecords + "}";

                      }

                  return info;
        }


        /// <summary>
        /// 获取账单信息
        /// </summary>
        /// <param name="pagesize"></param>
        /// <param name="currentpage"></param>
        /// <param name="search"></param>
        /// <returns></returns>
        public static string getSettlementInfo(int pagesize, int currentpage, string search)
        {
            string sql = null;
            string paramstr="";
            string cpsql = null;
            string info = "{data:[]}";
            
            Time tm=new Time();
            if (null != search)
            {
                JObject json = JObject.Parse(search);
                if (null != json["date"])
                {
                    string[] list = json["date"].ToString().Split(',');
                    for (int i = 0; i < list.Length; i++)
                        paramstr += " or (a.paydate bteween '" + list[i] + "' and '" + tm.LastDayOfMonth(Convert.ToDateTime(list[i])) + "'";
                }
                else
                    paramstr += "(a.paydate bteween '" + (DateTime.Now.Year.ToString() + "-01-01") + "' and '" + DateTime.Now.AddDays(-1).ToShortDateString() + "'";

                if (null != json["payflag"])
                    paramstr += " and payflag=" + json["payflag"].ToString();

                if (null != json["cpflag"])
                {
                    paramstr += " and cpflag=" + json["cpflag"].ToString();
                    if (Convert.ToInt32(json["cpflag"]) == 0)
                        cpsql = "select ','+convert(varchar(10),infoid) from conduit where name like '%{0}%' and pid=0 FOR XML PATH('')";
                    else if(Convert.ToInt32(json["cpflag"]) == 1)
                        cpsql = "select ','+convert(varchar(10),infoid) from company where name like '%{0}%' and pid=0 FOR XML PATH('')";
                }

                if (null != json["cpname"])
                {
                    DataTable cp=SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction,CommandType.Text,string.Format(cpsql,json["cpflag"].ToString())).Tables[0];
                    if(cp.Rows.Count>0)
                        paramstr += " and a.companyid in(" + cp.Rows[0][0].ToString().Substring(1) + ")";
                }
            }
            else
                paramstr += "(a.paydate bteween '" + (DateTime.Now.Year.ToString() + "-01-01") + "' and '" + DateTime.Now.AddDays(-1).ToShortDateString() + "'";


            int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "SELECT COUNT(a.infoid) FROM settlementInfo as a WITH(NOLOCK) WHERE "+ paramstr));
            if (currentpage == 1)
                sql = "select top 10 *,case cpflag=0 then (select top 1 names from conduit where infoid=a.companyid and pid=0) else (select top 1 name from company where infoid=a.companyid and pid=0) end as cpname,"
                    + "from settlementInfo as a WITH(NOLOCK) where a.infoid WHERE "+ paramstr + " ORDER BY a.infoid DESC";
            else
                sql = "select top " + pagesize + " *,case cpflag=0 then (select top 1 names from conduit where infoid=a.companyid and pid=0) else (select top 1 name from company where infoid=companyid and pid=0) end as cpname,"
                    + "from settlementInfo as a WITH(NOLOCK) WHERE a.infoid<=(SELECT MIN(infoid)"
                    + "FROM (SELECT TOP " + ((currentpage - 1) * pagesize + 1) + " infoid FROM settlementInfo WITH(NOLOCK) "
                    + "WHERE " + paramstr + " ORDER BY infoid DESC) AS [tblTmp]) "
                    + paramstr + " ORDER BY a.infoid DESC";

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",total:" + TotalRecords + "}";

            return info;
        }


        /// <summary>
        /// 获取账单明细
        /// </summary>
        /// <param name="pid"></param>
        /// <returns></returns>
        public static string getSettlementSheet(int pid)
        {
           string info = "{data:[]}";
           string sql = "select a.*,"
              + "(select top 1 names from conduit where infoid=a.conduitid) conduit,(select top 1 name from product where infoid=a.productid) as product "
              + "from settlementSheet as a WITH(NOLOCK) where a.pid=" + pid + " ORDER BY a.infoid ASC";

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + "}";

            return info;
        }


        /// <summary>
        /// 创建账单数据
        /// </summary>
        /// <returns></returns>
        public static bool creatSettlementData(string data, int cpflag)
        {
            bool flag = true;
            if (!string.IsNullOrEmpty(data))
            {

                JArray items = JArray.Parse(data);
                if (items.Count > 0)
                {
                    SqlConnection conn = new SqlConnection(SqlHelper.ConnectionStringLocalTransaction);
                    conn.Open();
                    using (SqlTransaction trans = conn.BeginTransaction())
                    {
                        try
                        {
                            Time tm = new Time();

                            for (int i = 0; i < items.Count; i++)
                            {
                                string streamingNo = GUID.GenerateUniqueID();
                                string between = null;
                                string date = null;


                                string sql1 = "insert into settlementInfo([companyID],[streamingNo],[cpFlag]) "
                                  + "values(" + Convert.ToUInt32(items[i]["pid"]) + ",'" + streamingNo + "'," + cpflag + ");SELECT SCOPE_IDENTITY()";

                                int old = Utils.StrToInt(SqlHelper.ExecuteScalar(trans, CommandType.Text, sql1), 0);

                                JArray list = JArray.Parse(items[i]["data"].ToString());
                                for (int j = 0; j < list.Count; j++)
                                {
                                    if (Convert.ToInt32(items[i]["settlement"]) == 0)
                                    {
                                        date = tm.LastDayOfMonth(Convert.ToDateTime(items[i]["times"].ToString() + "-01")).ToShortDateString();
                                        between = "datatime between '" + items[i]["times"].ToString() + "-01' and '" + date + "'";
                                    }
                                    else if (Convert.ToInt32(items[i]["settlement"]) == 1)
                                    {
                                        date = items[i]["times"].ToString().Substring(12);
                                        between = "datatime between '" + items[i]["times"].ToString().Substring(12) + "' and '" + date + "'";
                                    }
                                    else
                                    {
                                        date = items[i]["times"].ToString();
                                        between = "datatime='" + date + "'";
                                    }

                                    string sql2 = "insert into settlementSheet([pid],[conduitID],[companyID],[productID],[divideInto],[settlement],[paycycle],[price],[amount],[cpFlag]) "
                                         + "values(" + old + "," + list[j]["conduitID"].ToString() + "," + list[j]["companyID"].ToString() + "," + list[j]["productID"].ToString() + "," + list[j]["divideInto"].ToString() + "," + list[j]["settlement"].ToString() + ",'" + list[j]["times"].ToString() + "'," + list[j]["信息费"].ToString() + "," + list[j]["结算金额"].ToString() + "," + cpflag + ")";

                                    string sql3 = "update public_statistics set " + (cpflag == 1 ? "pflag=1" : "rflag=1") + " where " + between + " and companyid=" + list[j]["companyID"].ToString() + " and productid=" + list[j]["productID"].ToString();

                                    int num2 =SqlHelper.ExecuteNonQuery(trans, CommandType.Text, sql2);

                                    int num3 = SqlHelper.ExecuteNonQuery(trans, CommandType.Text, sql3);

                                }
                            }
                            trans.Commit();
                        }
                        catch (Exception e)
                        {
                            flag = false;
                            trans.Rollback();
                            LogHelper.WriteLog(typeof(dataManage), "============>creatSettlementData异常[" + e.ToString() + "]");
                        }
                    }
                    conn.Close();
                }
            }
            return flag; 
        }


        /// <summary>
        /// 获取账单
        /// </summary>
        ///<param name="currentpage"></param>
        ///<param name="pagesize"></param>
        ///<param name="search"></param>
        /// <returns></returns>
        public static string getBillInfo(int pagesize, int currentpage, string search)
        {
            string sql = null;
            string paramstr=null;
            string info = "{data:[]}";

            if (null != search)
            {
                JObject json = JObject.Parse(search);
                paramstr = "a.cpFlag=" + json["cpflag"].ToString();
                if (null != json["date"])
                {
                    string[] list = json["date"].ToString().Split(',');
                    if (Convert.ToInt32(json["types"]) == 1 && null == json["word"])
                    {
                        for (int i = 0; i < list.Length; i++)
                            paramstr += " or paycycle like '%" + list[i].Substring(0, 7) + "%'";
                        paramstr = "a.companyid in (select companyid from settlementSheet where" + paramstr.Substring(3)+" group by companyid)";
                    }
                    else
                    {
                        for (int i = 0; i < list.Length; i++)
                            paramstr += " or streamingNo like '" + list[i].Substring(0, 7).Replace("-","") + "%'";
                        paramstr = " and (" + paramstr.Substring(3) + ")";
                    }
                }
                if (null != json["word"])
                    paramstr += " and a.companyid in(select infoid from company where names like '%" + json["word"].ToString() + "%' group by infoid)";
                if(null!=json["payflag"])
                    paramstr += " and a.payFlag in(" + json["payflag"].ToString() + ")";
                
            }
            int TotalRecords = Convert.ToInt32(SqlHelper.ExecuteScalar(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, "SELECT COUNT(a.infoid) FROM settlementInfo as a WITH(NOLOCK) WHERE " + paramstr));
            if (currentpage == 1)
                sql = "SELECT TOP " + pagesize + " a.infoid,a.companyID,a.streamingNo,a.attach,a.paydate,a.cpFlag,a.payFlag,"
                    +"(select sum(amount) from settlementSheet where pid=a.infoid) as amount,"
                    + "(select top 1 name from company where infoid=a.companyid) as names "
                    + "FROM settlementInfo as a WITH(NOLOCK) "
                    + "where " + paramstr + " ORDER BY a.infoid DESC";
            else
            {
                sql = "SELECT TOP " + pagesize + " a.infoid,a.companyID,a.streamingNo,a.attach,a.paydate,a.cpFlag,a.payFlag,"
                    +"(select sum(amount) from settlementSheet where pid=a.infoid) as amount,"
                    + "(select top 1 name from company where infoid=a.companyid) as names "
                    + "FROM settlementInfo as a WITH(NOLOCK) WHERE a.infoid <=(SELECT MIN(infoid) "
                    + "FROM (SELECT TOP " + ((currentpage - 1) * pagesize + 1) + " [infoid] FROM settlementInfo as a WITH(NOLOCK) "
                    + "WHERE " + paramstr.Replace("a.", "") + " ORDER BY [infoid] DESC) AS [tblTmp]) "
                    + paramstr + " ORDER BY a.infoid DESC";
            }
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:" + JsonConvert.SerializeObject(dt, new DataTableConverter()) + ",total:" + TotalRecords + "}";


            return info;
        }


        /// <summary>
        /// 获取支付账单备注
        /// </summary>
        /// <param name="infoid"></param>
        /// <returns></returns>
        public static string getBillRemark(int infoid)
        {
            string info = null;
            string sql = "select remarks from settlementInfo where infoid=" + infoid;
            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];

            if (dt.Rows.Count > 0)
                info = "{data:"+JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[","").Replace("]","")+",\"success\":true}";


            return info;
        }

        /// <summary>
        /// 统计下游业务状态
        /// </summary>
        /// <param name="priductid"></param>
        /// <returns></returns>
        public static string getStartFlagCount(int infoid)
        {
            string sql = string.Format("select startFlag as flag from product where infoid={0}", infoid);

            DataTable dt = getProductInfo(infoid);

            return  JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");
        }


        public static string getSettlementType(int cid, int pid)
        {
            string info = "{'settlement':null}";
            string sql = string.Format("select settlement from action where productid={0} and companyid={1}", pid,cid);

            DataTable dt = SqlHelper.ExecuteDataset(SqlHelper.ConnectionStringLocalTransaction, CommandType.Text, sql).Tables[0];
            if (dt.Rows.Count > 0)
                info = JsonConvert.SerializeObject(dt, new DataTableConverter()).Replace("[", "").Replace("]", "");

            return info;
        }

    }
}
