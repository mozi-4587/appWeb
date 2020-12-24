using System;
using System.Collections.Generic;
using System.Text;
using app.Entity;
using app.Common;
using app.Manage;
using System.Collections;
using System.Data;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;


namespace app.Service
{
    public class Rules
    {
        public static ConcurrentDictionary<string, string> feeDict = new ConcurrentDictionary<string, string>();
        public static ConcurrentDictionary<string, string> areaDict = new ConcurrentDictionary<string, string>();
        public static ConcurrentDictionary<string, string> wordDict = new ConcurrentDictionary<string, string>();
        public static ConcurrentDictionary<string, string> otherDict = new ConcurrentDictionary<string, string>();

        private static string keyp = "{0}-{1}-{2}";//conduitid-productid-typeid
        private static string keyc = "{0}-{1}-{2}-{3}";//conduitid-productid-typeid-companyid
        private static string keya = "{0}-{1}-{2}-{3}-{4}";//conduitid-productid-typeid-companyid-setfee


        /// <summary>
        /// 限量抓取设置(同步时发生)
        /// </summary>
        /// <param name="limit"></param>
        /// <param name="key"></param>
        /// <param name="data"></param>
        static void setWordDictNotify(string limit, string key, string word, string area)
        {
            string province = null;
            string datatime = DateTime.Now.ToString("yyyy-MM");
            if (string.IsNullOrEmpty(limit))
                return;

            string[] areainfo = area.Split(' ');
            province = areainfo[0];

            if (limit.IndexOf(word) > -1)
            {
                JArray json = JArray.Parse(limit);

                foreach (JObject itme in json)
                {
                    string info = itme["word"].ToString();
                    if (word.IndexOf(info) > -1)
                    {
                        if (itme["flag"].ToString() == "9999")
                        {
                            string itemValue = null;
                            bool isItem = wordDict.TryGetValue(string.Format("{0}-9999", key), out itemValue);
                            if (isItem)
                            {
                                if (itemValue.IndexOf(datatime) > -1)
                                    wordDict.TryUpdate(string.Format("{0}-9999", key), json.Remove("day").ToString(), itemValue);
                            }
                            else
                                wordDict.TryAdd(string.Format("{0}-9999", key), json.Remove("day").ToString());

                            break;
                        }
                        else if (itme["flag"].ToString() == "999")
                        {
                            string itemValue = null;
                            bool isItem = wordDict.TryGetValue(string.Format("{0}-999", key), out itemValue);
                            if (isItem)
                            {
                                if (itemValue.IndexOf(datatime) > -1)
                                    wordDict.TryUpdate(string.Format("{0}-999", key), json.Remove("month").ToString(), itemValue);
                            }
                            else
                                wordDict.TryAdd(string.Format("{0}-999", key), json.Remove("month").ToString());

                            break;
                        }
                        else if (itme["flag"].ToString() == "99")
                        {
                            string itemValue = null;
                            bool isItem = wordDict.TryGetValue(key, out itemValue);
                            if (isItem)
                            {
                                if (itemValue.IndexOf(datatime) > -1)
                                    wordDict.TryUpdate(key, json.Remove("month").ToString(), itemValue);
                            }
                            else
                                wordDict.TryAdd(key, json.Remove("month").ToString());

                            break;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 限量抓取设置(请求时发生)
        /// </summary>
        /// <param name="limit"></param>
        /// <param name="key"></param>
        /// <param name="word"></param>
        /// <returns></returns>
        static string setWordDict(string limit, string key, string word, string area)
        {
            string province = null;
            string datatime = DateTime.Now.ToString("yyyy-MM");
            if (string.IsNullOrEmpty(limit))
                return null;

            string[] areainfo = area.Split(' ');
            province = areainfo[0];

            if (limit.IndexOf(word) > -1)
            {
                JArray json = JArray.Parse(limit);

                foreach (JObject itme in json)
                {
                    string info = itme["word"].ToString();
                    if (word.IndexOf(info) > -1)
                    {
                        if (itme["flag"].ToString() == "9999")
                        {
                            string itemValue = null;
                            bool isItem = wordDict.TryGetValue(string.Format("{0}-9999", key), out itemValue);
                            if (isItem)
                            {
                                if (itemValue.IndexOf(datatime) > -1)
                                {
                                    wordDict.TryUpdate(string.Format("{0}-9999", key), json.Remove("day").ToString(), itemValue);
                                    return "{\"resultCode\":\"9999\",\"status\":\"本月通道量满\"}";
                                }
                            }
                            else
                            {
                                wordDict.TryAdd(string.Format("{0}-9999", key), json.Remove("day").ToString());
                                return "{\"resultCode\":\"9999\",\"status\":\"本月通道量满\"}";
                            }
                        }
                        else if (itme["flag"].ToString() == "999")
                        {
                            string itemValue = null;
                            bool isItem = wordDict.TryGetValue(string.Format("{0}-999", key), out itemValue);
                            if (isItem)
                            {
                                if (itemValue.IndexOf(datatime) > -1)
                                {
                                    wordDict.TryUpdate(string.Format("{0}-999", key), json.Remove("month").ToString(), itemValue);
                                    return "{\"resultCode\":\"999\",\"status\":\"本日通道量满\"}";
                                }
                            }
                            else
                            {
                                wordDict.TryAdd(string.Format("{0}-999", key), json.Remove("month").ToString());
                                return "{\"resultCode\":\"999\",\"status\":\"本日通道量满\"}";
                            }
                        }
                        else if (itme["flag"].ToString() == "99")
                        {
                            string itemValue = null;
                            bool isItem = wordDict.TryGetValue(key, out itemValue);
                            if (isItem)
                            {
                                if (itemValue.IndexOf(datatime) > -1)
                                {
                                    if (itemValue.IndexOf(province) == -1)
                                    {
                                        JObject jsoninfo = JObject.Parse(itemValue);
                                        if (!string.IsNullOrEmpty(itme["area"].ToString()))
                                            jsoninfo["area"] = jsoninfo["area"].ToString() + "," + province;
                                        else
                                            jsoninfo["area"] = province;

                                        areaDict.TryUpdate(key, jsoninfo.ToString(), itemValue);

                                        return "{\"resultCode\":\"99\",\"status\":\"" + province + "省份量满\"}";
                                    }

                                    wordDict.TryUpdate(key, json.Remove("month").ToString(), itemValue);
                                    return "{\"resultCode\":\"99\",\"status\":\"" + province + "省份量满\"}";
                                }
                            }
                            else
                            {
                                wordDict.TryAdd(key, json.Remove("month").ToString());
                                return "{\"resultCode\":\"99\",\"status\":\"" + province + "省份量满\"}";
                            }
                        }
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// 总量限制设置规则（提交验证码时发生）
        /// </summary>
        /// <param name="limit"></param>
        /// <param name="key"></param>
        /// <param name="data"></param>
        static string setFeeDict( string limit, string key, DataTable data)
        {
            string datatime = DateTime.Now.ToString("yyyy-MM-dd");
            Time month = new Time();
            string date_m = month.GetMonthLastDate();

            if (string.IsNullOrEmpty(limit))
                return null;

            JObject json = JObject.Parse(limit);

            string tabname = "public_order_2017";

            DataTable info = Service.getLimitTotal(Convert.ToInt32(data.Rows[0]["conduitID"]), Convert.ToInt32(data.Rows[0]["productID"]), Convert.ToInt32(data.Rows[0]["companyID"]), null, null, Convert.ToInt32(json["fee"]),data.Rows[0]["code"].ToString(), json["field"].ToString(), Convert.ToBoolean(json["filter"]), tabname);
            if (info.Rows.Count>0)
            {
                if (null != json["month"])
                {
                    string itemValue = null;
                    if (Convert.ToInt32(info.Rows[0]["month"]) >= Convert.ToInt32(json["month"]))
                        json["month"] = Convert.ToInt32(info.Rows[0]["month"]);

                    bool isItem = feeDict.TryGetValue(string.Format("{0}-9999", key), out itemValue);
                    if (isItem)
                    {
                        if (itemValue.IndexOf(datatime.Substring(0, 7)) > -1)
                        {
                            feeDict.TryUpdate(string.Format("{0}-9999", key), json.Remove("day").ToString(), itemValue);
                            return "{\"resultCode\":\"9999\",\"status\":\"本月通道量满\"}";
                        }
                    }
                    else
                    {
                        feeDict.TryAdd(string.Format("{0}-9999", key), json.Remove("day").ToString());
                        return "{\"resultCode\":\"9999\",\"status\":\"本月通道量满\"}";
                    }
                }
                if (null != json["day"])
                {
                    string itemValue = null;
                    if (Convert.ToInt32(info.Rows[0]["day"]) >= Convert.ToInt32(json["day"]))
                        json["day"] = Convert.ToInt32(info.Rows[0]["day"]);
                    
                    bool isItem = feeDict.TryGetValue(string.Format("{0}-999", key), out itemValue);
                    if (isItem)
                    {
                        if (itemValue.IndexOf(datatime) > -1)
                        {
                            feeDict.TryUpdate(string.Format("{0}-999", key), json.Remove("month").ToString(), itemValue);
                            return "{\"resultCode\":\"999\",\"status\":\"本日通道量满\"}";
                        }
                    }
                    else
                    {
                        feeDict.TryAdd(string.Format("{0}-999", key), json.Remove("month").ToString());
                        return "{\"resultCode\":\"999\",\"status\":\"本日通道量满\"}";
                    }
                }
            }

            return null;
        }


        /// <summary>
        /// 总量限制设置规则（同步时发生）
        /// </summary>
        /// <param name="limit"></param>
        /// <param name="key"></param>
        /// <param name="data"></param>
        static void setFeeDictNofity(string limit, string key, DataTable data)
        {
            string datatime = DateTime.Now.ToString("yyyy-MM-dd");
            Time month = new Time();
            string date_m = month.GetMonthLastDate();

            if (string.IsNullOrEmpty(limit))
                return ;

            JObject json = JObject.Parse(limit);

            string tabname = "public_notify_2017";

            DataTable info = Service.getLimitTotal(Convert.ToInt32(data.Rows[0]["conduitID"]), Convert.ToInt32(data.Rows[0]["productID"]), Convert.ToInt32(data.Rows[0]["companyID"]), null, null, Convert.ToInt32(json["fee"]), null, json["field"].ToString(), Convert.ToBoolean(json["filter"]), tabname);
            if (info.Rows.Count > 0)
            {
                if (null != json["month"])
                {
                    string itemValue = null;
                    if (Convert.ToInt32(info.Rows[0]["month"]) >= Convert.ToInt32(json["month"]))
                        json["month"] = Convert.ToInt32(info.Rows[0]["month"]);

                    bool isItem = feeDict.TryGetValue(string.Format("{0}-9999", key), out itemValue);
                    if (isItem)
                    {
                        if (itemValue.IndexOf(datatime.Substring(0, 7)) > -1)
                            feeDict.TryUpdate(string.Format("{0}-9999", key), json.Remove("day").ToString(), itemValue);
                    }
                    else
                       feeDict.TryAdd(string.Format("{0}-9999", key), json.Remove("day").ToString());
                }
                if (null != json["day"])
                {
                    string itemValue = null;
                    if (Convert.ToInt32(info.Rows[0]["day"]) >= Convert.ToInt32(json["day"]))
                        json["day"] = Convert.ToInt32(info.Rows[0]["day"]);

                    bool isItem = feeDict.TryGetValue(string.Format("{0}-999", key), out itemValue);
                    if (isItem)
                    {
                        if (itemValue.IndexOf(datatime) > -1)
                          feeDict.TryUpdate(string.Format("{0}-999", key), json.Remove("month").ToString(), itemValue);
                    }
                    else
                        feeDict.TryAdd(string.Format("{0}-999", key), json.Remove("month").ToString());
                }
            }
        }

        /// <summary>
        /// 省份自定义限制(同步时发生)
        /// </summary>
        /// <param name="limit"></param>
        /// <param name="key"></param>
        /// <param name="data"></param>
        static void setAreaDictNotify(string limit, string key, DataTable data)
        {
            string province = null;
            string itemValue = null;
            DataTable number = new DataTable();
            string datatime = DateTime.Now.ToString("yyyy-MM-dd");

            string tabname = "public_notify_2017";

            if (string.IsNullOrEmpty(limit))
                return;

            JObject areajson = JObject.Parse(limit);
            JArray json = JArray.Parse(areajson["data"].ToString());

            if (string.IsNullOrEmpty(data.Rows[0]["area"].ToString().Trim()))
                return;

            string[] area = data.Rows[0]["area"].ToString().Split(' ');
            province = area[0];
            if (areajson["data"].ToString().IndexOf(province) > -1)
            {
                bool isItem = areaDict.TryGetValue(key, out itemValue);
                if (isItem)
                {
                    JObject jsoninfo = JObject.Parse(itemValue);
                    if (itemValue.IndexOf(datatime) > -1)
                    {
                        if (itemValue.IndexOf(province) == -1)
                        {
                            if (!string.IsNullOrEmpty(jsoninfo["area"].ToString()))
                                jsoninfo["area"] = jsoninfo["area"].ToString() + "," + province;
                            else
                                jsoninfo["area"] = province;
                            areaDict.TryUpdate(key, jsoninfo.ToString(), itemValue);
                        }
                    }
                    else
                    {
                        jsoninfo["datatime"] = datatime;
                        jsoninfo["area"] = province;
                        areaDict.TryUpdate(key, jsoninfo.ToString(), itemValue);
                    }
                }
                else
                {
                    foreach (JObject item in json)
                    {
                        string areainfo = item["area"].ToString();
                        if (areainfo.IndexOf(province) > -1)
                        {
                            string[] keyitem = key.Split('-');

                            if (keyitem.Length == 4)//优先级最高
                                number = Service.getLimitTotal(Convert.ToInt32(keyitem[0]), Convert.ToInt32(keyitem[1]), Convert.ToInt32(keyitem[3]), null, province, Convert.ToInt32(keyitem[4]), null, areajson["field"].ToString(), Convert.ToBoolean(areajson["filter"]), tabname);
                            else if (keyitem.Length == 3)
                                number = Service.getLimitTotal(Convert.ToInt32(keyitem[0]), Convert.ToInt32(keyitem[1]), Convert.ToInt32(keyitem[3]), null, province, 0, null, areajson["field"].ToString(), Convert.ToBoolean(areajson["filter"]), tabname);
                            else if (keyitem.Length == 2)//优先级最低
                                number = Service.getLimitTotal(Convert.ToInt32(keyitem[0]), Convert.ToInt32(keyitem[1]), 0, null, province, 0, null, areajson["field"].ToString(), Convert.ToBoolean(areajson["filter"]), tabname);

                            if (number.Rows.Count > 0)
                            {
                                if (Convert.ToInt32(number.Rows[0]["number"]) >= Convert.ToInt32(item["day"]))
                                {
                                    item["datatime"] = datatime;
                                    item["area"] = province;
                                    areaDict.TryAdd(key, item.ToString());
                                }
                            }
                        }
                    }
                }
            }
        }


        /// <summary>
        /// 省份自定义限制(提交验证码时发生)
        /// </summary>
        /// <param name="limit"></param>
        /// <param name="key"></param>
        /// <param name="data"></param>
        static string setAreaDict(string limit, string key, DataTable data)
        {
            string province = null;
            string itemValue = null;
            DataTable number = new DataTable();
            string datatime = DateTime.Now.ToString("yyyy-MM-dd");

            string tabname = "public_order_2017";

            if (string.IsNullOrEmpty(limit))
                return null;

            JObject areajson = JObject.Parse(limit);
            JArray json = JArray.Parse(areajson["data"].ToString());

            if (string.IsNullOrEmpty(data.Rows[0]["area"].ToString().Trim()))
                return null;

            string[] area = data.Rows[0]["area"].ToString().Split(' ');
            province = area[0];
            if (areajson["data"].ToString().IndexOf(province) > -1)
            {
                bool isItem = areaDict.TryGetValue(key, out itemValue);
                if (isItem)
                {
                        JObject jsoninfo = JObject.Parse(itemValue);
                        if (itemValue.IndexOf(datatime) > -1)
                        {
                            if (itemValue.IndexOf(province) == -1)
                            {
                                if (!string.IsNullOrEmpty(jsoninfo["area"].ToString()))
                                    jsoninfo["area"] = jsoninfo["area"].ToString() + "," + province;
                                else
                                    jsoninfo["area"] = province;
                                areaDict.TryUpdate(key, jsoninfo.ToString(), itemValue);

                                return "{\"resultCode\":\"99\",\"status\":\"" + province + "省份量满\"}";
                            }
                        }
                        else
                        {
                            jsoninfo["datatime"] = datatime;
                            jsoninfo["area"] = province;
                            areaDict.TryUpdate(key, jsoninfo.ToString(), itemValue);

                            return "{\"resultCode\":\"99\",\"status\":\"" + province + "省份量满\"}";
                        }
                }
                else
                {
                    foreach (JObject item in json)
                    {
                        string areainfo= item["area"].ToString();
                        if (areainfo.IndexOf(province) > -1)
                        {
                            string[] keyitem = key.Split('-');

                            if (keyitem.Length == 4)//优先级最高
                                number = Service.getLimitTotal(Convert.ToInt32(keyitem[0]), Convert.ToInt32(keyitem[1]), Convert.ToInt32(keyitem[3]), null, province, Convert.ToInt32(keyitem[4]), data.Rows[0]["code"].ToString(), areajson["field"].ToString(), Convert.ToBoolean(areajson["filter"]), tabname);
                            else if (keyitem.Length == 3)
                                number = Service.getLimitTotal(Convert.ToInt32(keyitem[0]), Convert.ToInt32(keyitem[1]), Convert.ToInt32(keyitem[3]), null, province, 0, data.Rows[0]["code"].ToString(), areajson["field"].ToString(), Convert.ToBoolean(areajson["filter"]), tabname);
                            else if (keyitem.Length == 2)//优先级最低
                                number = Service.getLimitTotal(Convert.ToInt32(keyitem[0]), Convert.ToInt32(keyitem[1]), 0, null, province, 0, data.Rows[0]["code"].ToString(), areajson["field"].ToString(), Convert.ToBoolean(areajson["filter"]), tabname);

                            if (number.Rows.Count > 0)
                            {
                                if (Convert.ToInt32(number.Rows[0]["number"]) >= Convert.ToInt32(item["day"]))
                                {
                                    item["datatime"] = datatime;
                                    item["area"] = province;
                                    areaDict.TryAdd(key, item.ToString());

                                    return "{\"resultCode\":\"99\",\"status\":\"" + province + "省份量满\"}";
                                }
                            }
                        }
                    }
                }
            }
           
            return null;
        }


        /// <summary>
        /// 同步时设置规则
        /// </summary>
        /// <param name="data"></param>
        /// <param name="typeList"></param>
        public static void setRuleInfo(DataTable data, int step, string typeList)
        {
            /*string datatime = DateTime.Now.ToString("yyyy-MM-dd");
            Time month = new Time();
            string date_m = month.GetMonthLastDate();*/

            string[] area = data.Rows[0]["area"].ToString().Split(' ');

            DataTable rules = Service.getJsonRules(Convert.ToInt32(data.Rows[0]["productid"]), typeList);
            string[] group = typeList.Split(',');
            if (group.Length > 0)
            {
                foreach (string dr in group)
                {
                    string limit = null;
                    string key = null;
                    int typeid = Convert.ToInt32(dr);
                    
                    DataRow[] a_rules = rules.Select("typeid=" + dr + " and companyid=" + Convert.ToInt32(data.Rows[0]["companyID"]) + " and setfee=" + Convert.ToInt32(data.Rows[0]["price"]) + " and setfee>0");//a_rules 优先级最高
                    DataRow[] c_rules = rules.Select("typeid=" + dr + " and companyid=" + Convert.ToInt32(data.Rows[0]["companyID"])+" and setfee=0");
                    DataRow[] p_rules = rules.Select("typeid=" + dr);//a_rules 优先级最低

                    if (a_rules.Length > 0)
                    {
                        limit = a_rules[0]["limit"].ToString();
                        key = string.Format(keya, data.Rows[0]["conduitID"].ToString(), data.Rows[0]["productID"].ToString(), data.Rows[0]["companyID"].ToString(), dr, data.Rows[0]["price"].ToString());
                    }
                    else if (c_rules.Length > 0)
                    {
                        limit = c_rules[0]["limit"].ToString();
                        key = string.Format(keyc, data.Rows[0]["conduitID"].ToString(), data.Rows[0]["productID"].ToString(), data.Rows[0]["companyID"].ToString(), dr);
                    }
                    else
                    {
                        limit = p_rules[0]["limit"].ToString();
                        key = string.Format(keyp, data.Rows[0]["conduitID"].ToString(), data.Rows[0]["productID"].ToString(), dr);
                    }
                    if (string.IsNullOrEmpty(limit.Trim()))
                        return;

                    if (typeid == 2)//省份限制
                    {
                        if (step == 3)
                        {
                            if (a_rules.Length > 0)
                                setAreaDictNotify(a_rules[0]["limit"].ToString(), key, data);

                            else if (c_rules.Length > 0)
                                setAreaDictNotify(c_rules[0]["limit"].ToString(), key, data);

                            else if (p_rules.Length > 0)
                                setAreaDictNotify(p_rules[0]["limit"].ToString(), key, data);
                        }
                    }
                    else if (typeid == 1)//抓取限制
                    {
                        if (a_rules.Length > 0)
                            setWordDictNotify(a_rules[0]["limit"].ToString(), key, data.Rows[0]["notifypack"].ToString(), data.Rows[0]["area"].ToString());
                        
                        else if (c_rules.Length > 0)
                            setWordDictNotify(c_rules[0]["limit"].ToString(), key, data.Rows[0]["notifypack"].ToString(), data.Rows[0]["area"].ToString());
                        
                        else if (p_rules.Length > 0)
                            setWordDictNotify(p_rules[0]["limit"].ToString(), key, data.Rows[0]["notifypack"].ToString(), data.Rows[0]["area"].ToString());
                    }
                    else if (typeid == 3)//总量限制
                    {
                        if (step== 3)
                        {
                            if (a_rules.Length > 0)
                                setFeeDictNofity(a_rules[0]["limit"].ToString(), key, data);

                            else if (c_rules.Length > 0)
                                setFeeDictNofity(c_rules[0]["limit"].ToString(), key, data);

                            else if (p_rules.Length > 0)
                                setFeeDictNofity(p_rules[0]["limit"].ToString(), key, data);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 在提交验证码规则
        /// </summary>
        /// <param name="data"></param>
        /// <param name="typeList"></param>
        /// <returns></returns>
        public static string resultRuleInfo(DataTable data, string typeList)
        {
             if(data.Rows.Count==0)
                return null;

             string key = null;
             string area = data.Rows[0]["area"].ToString();
             DataTable rules = Service.getJsonRules(Convert.ToInt32(data.Rows[0]["productID"]), typeList);
             string[] group = typeList.Split(',');
             string datatime = DateTime.Now.ToString("yyyy-MM-dd");
             if (group.Length > 0)
             {
                 foreach (string dr in group)
                 {
                    string limit = null;
                    int typeid=Convert.ToInt32(dr);
                     
                    DataRow[] a_rules = rules.Select("typeid=" + dr + " and companyid=" + Convert.ToInt32(data.Rows[0]["companyID"]) + " and setfee=" + Convert.ToInt32(data.Rows[0]["price"]));//a_rules 优先级最高
                    DataRow[] c_rules = rules.Select("typeid=" + dr + " and companyid=" + Convert.ToInt32(data.Rows[0]["companyID"])+ " and setfee=0");
                    DataRow[] p_rules = rules.Select("typeid=" + dr);//a_rules 优先级最低

                    if (a_rules.Length > 0)
                    {
                        limit = a_rules[0]["limit"].ToString();
                        key = string.Format(keya, data.Rows[0]["conduitID"].ToString(), data.Rows[0]["productID"].ToString(), data.Rows[0]["companyID"].ToString(), dr, data.Rows[0]["price"].ToString());
                    }
                    else if (c_rules.Length > 0)
                    {
                        limit = c_rules[0]["limit"].ToString();
                        key = string.Format(keyc, data.Rows[0]["conduitID"].ToString(), data.Rows[0]["productID"].ToString(), data.Rows[0]["companyID"].ToString(), dr);
                    }
                    else
                    {
                        limit = p_rules[0]["limit"].ToString();
                        key = string.Format(keyp, data.Rows[0]["conduitID"].ToString(), data.Rows[0]["productID"].ToString(), dr);
                    }
                    if (string.IsNullOrEmpty(limit.Trim()))
                        return null;

                    if (typeid == 1)//读取抓取规则
                        return setWordDict(limit, key, data.Rows[0]["dowpack"].ToString(), data.Rows[0]["area"].ToString());

                    else if (typeid == 2)//设置省份规则
                    {
                        JObject json = JObject.Parse(limit);
                        if (Convert.ToInt32(json["step"]) == 2)
                        {
                            if (a_rules.Length > 0)
                                return setAreaDict(limit, key, data);
                        }
                    }
                    else if (typeid == 3)//设置总量规则
                    {
                        JObject json = JObject.Parse(limit);
                        if (Convert.ToInt32(json["step"]) == 2)
                        {
                            if (a_rules.Length > 0)
                                return setFeeDict(limit, key, data);
                        }
                    }
                    else if (typeid == 5)//设置验证码提交限制
                    {
                        JObject json = JObject.Parse(limit);
                        int companyID = 0;
                        int fee = 0;
                        if (json["field"].ToString() == "companyid")
                            companyID = Convert.ToInt32(data.Rows[0]["productID"]);
                        else if (json["field"].ToString() == "fee")
                        {
                            companyID = Convert.ToInt32(data.Rows[0]["productID"]);
                            fee = Convert.ToInt32(data.Rows[0]["fee"]);
                        }
                        string tabname = "public_order_2017";

                        DataTable info = Service.getLimitTotal(Convert.ToInt32(data.Rows[0]["conduitID"]), Convert.ToInt32(data.Rows[0]["productID"]), companyID, null, null, fee, data.Rows[0]["code"].ToString(), json["field"].ToString(), Convert.ToBoolean(json["filter"]), tabname);
                        if (info.Rows.Count > 0)
                        {
                            if (null != json["month"])
                            {
                                if (Convert.ToInt32(info.Rows[0]["number"]) >= Convert.ToInt32(json["month"]))
                                    return "{\"resultCode\":\"1\",\"status\":\"提交月限\"}";
                            }
                            else
                            {
                                if (Convert.ToInt32(info.Rows[0]["number"]) >= Convert.ToInt32(json["day"]))
                                  return "{\"resultCode\":\"1\",\"status\":\"号码日限\"}";
                            }
                        }
                        
                        /*if (!string.IsNullOrEmpty(datainfo))
                        {
                            JObject temp = JObject.Parse(datainfo);
                            if (null != json["month"])
                            {
                                json.Add(new JProperty("datatime", date_m));
                                otherDict.TryAdd(key + "-m", json.ToString().Replace(" ", ""));
                                return "{\"resultCode\":\"1\",\"status\":\"号码月限\"}";
                            }
                            else
                            {
                                json.Add(new JProperty("datatime", datatime));
                                otherDict.TryAdd(key, json.ToString().Replace(" ", ""));
                                return "{\"resultCode\":\"1\",\"status\":\"号码日限\"}";
                            }
                        }*/
                    }
                     /*ArrayList list = new ArrayList();

                     list.Add(string.Format(keya, data.Rows[0]["contuidID"].ToString(), data.Rows[0]["productID"].ToString(), dr, Convert.ToInt32(data.Rows[0]["companyID"]), Convert.ToInt32(data.Rows[0]["fee"])));
                     list.Add(string.Format(keyc, data.Rows[0]["contuidID"].ToString(), data.Rows[0]["productID"].ToString(), dr, data.Rows[0]["companyID"].ToString()));
                     list.Add(string.Format(keyp, data.Rows[0]["contuidID"].ToString(), data.Rows[0]["productID"].ToString(), dr));
                    
                     string area = data.Rows[0]["area"].ToString();

                     if (typeid == 1)//读取
                     {
                         for (int i = 0; i < list.Count; i++)
                         {
                             string[] tempKey = { string.Format("{0}-9999", list[i].ToString()), string.Format("{0}-999", list[i].ToString()), list[i].ToString() };
                             for (int j = 0; j < tempKey.Length; j++)
                             {
                                 string itemValue = null;
                                 bool isItem = wordDict.TryGetValue(tempKey[j], out itemValue);
                                 if (!string.IsNullOrEmpty(itemValue))
                                 {
                                     if (itemValue.IndexOf(datatime) > -1)
                                     {
                                         if (j == 0)
                                             return "{\"resultCode\":\"9999\",\"status\":\"本月通道量满\"}";
                                         else if (j == 1)
                                             return "{\"resultCode\":\"999\",\"status\":\"今日通道量满\"}";
                                         else if (j == 2)
                                         {
                                             if (!string.IsNullOrEmpty(area.Trim()))
                                             {
                                                 string[] province = area.Split(' ');
                                                 if (itemValue.IndexOf(province[0]) > -1)
                                                     return "{\"resultCode\":\"99\",\"status\":\"" + province[0] + "省份量满\"}";
                                             }
                                         }
                                     }
                                 }
                             }
                         }
                     }
                     else if (typeid == 2)//读取
                     {
                         for (int i = 0; i < list.Count; i++)
                         {
                             string itemValue = null;
                             bool isItem = areaDict.TryGetValue(list[i].ToString(), out itemValue);
                             if (!string.IsNullOrEmpty(itemValue))
                             {
                                 if (!string.IsNullOrEmpty(area.Trim()))
                                 {
                                     string[] province = area.Split(' ');
                                     if (itemValue.IndexOf(province[0]) > -1)
                                         return "{\"resultCode\":\"99\",\"status\":\"" + province[0] + "省份量满\"}";
                                 }
                             }
                         }
                     }
                     else if (typeid==3)//总量日月限制-读取-设置
                     {
                         for (int i = 0; i < list.Count; i++)
                         {
                             string itemValue = null;
                             bool isItem = feeDict.TryGetValue(list[i].ToString(), out itemValue);
                             if (!string.IsNullOrEmpty(itemValue))
                             {
                                 if (itemValue.IndexOf("9999") > -1)
                                     return "{\"resultCode\":\"9999\",\"status\":\"本月通道量满\"}";

                                 else if (itemValue.IndexOf(datatime) > -1)
                                     return "{\"resultCode\":\"999\",\"status\":\"今日通道量满\"}";
                             }
                         }
                     }
                     else if (typeid == 5)//请求日月限制-读取-设置
                     {
                         for (int i = 0; i < list.Count; i++)
                         {
                             string itemValue = null;
                             bool isItem = otherDict.TryGetValue(list[i].ToString(), out itemValue);
                             if (!string.IsNullOrEmpty(itemValue))
                             {
                                 if (itemValue.IndexOf('m') > -1)
                                     return "{\"resultCode\":\"1\",\"status\":\"号码月限\"}";

                                 else if (itemValue.IndexOf(datatime) > -1)
                                     return "{\"resultCode\":\"1\",\"status\":\"号码日限\"}";
                             }
                         }
                     }*/
                 }
             }
             return null;
        }

        /// <summary>
        /// 请求时的规则(在请求时设置)
        /// </summary>
        /// <param name="action"></param>
        /// <param name="ht"></param>
        /// <param name="result"></param>
        /// <param name="typeList"></param>
        /// <param name="tablename"></param>
        /// <returns></returns>
        public static string resultRuleInfo(DataTable action, Hashtable ht, string result, string typeList,string tablename)
        {
            string key = null;
            string info = null;
            string datatime = DateTime.Now.ToString("yyyy-MM-dd");
            Time month = new Time();
            string date_m = month.GetMonthLastDate();

            if (Convert.ToInt32(action.Rows[0]["startFlag"]) == 0)
                return "{\"resultCode\":\"-1\",\"status\":\"通道未开通!\"}";

            if (Convert.ToInt32(action.Rows[0]["syncStart"]) == 0)
                return "{\"resultCode\":\"-1\",\"status\":\"通道未开通!\"}";

            mobileArea areainfo = new mobileArea();
            if (null != ht["Mobile"] && !string.IsNullOrEmpty(ht["Mobile"].ToString()))
                areainfo = Service.getMobileArea(ht["Mobile"].ToString());

            DataTable rules = Service.getJsonRules(Convert.ToInt32(action.Rows[0]["productid"]), typeList);
            string[] group = typeList.Split(',');
            if (group.Length > 0)
            {
                foreach (string dr in group)
                {
                    string limit = null;
                    int typeid = Convert.ToInt32(dr);
                    DataRow[] a_rules = rules.Select("typeid=" + dr + " and companyid=" + Convert.ToInt32(action.Rows[0]["companyID"]) + " and setfee=" + Convert.ToInt32(action.Rows[0]["price"]));//a_rules 优先级最高
                    DataRow[] c_rules = rules.Select("typeid=" + dr + " and companyid=" + Convert.ToInt32(action.Rows[0]["companyID"])+ " and setfee=0");
                    DataRow[] p_rules = rules.Select("typeid=" + dr);//a_rules 优先级最低

                    
                    if (a_rules.Length > 0)
                    {
                        limit = a_rules[0]["limit"].ToString();
                        key = string.Format(keya, action.Rows[0]["conduitID"].ToString(), action.Rows[0]["productID"].ToString(), action.Rows[0]["companyID"].ToString(), dr, action.Rows[0]["price"].ToString());
                    }
                    else if (c_rules.Length > 0)
                    {
                        limit = c_rules[0]["limit"].ToString();
                        key = string.Format(keyc, action.Rows[0]["conduitID"].ToString(), action.Rows[0]["productID"].ToString(), action.Rows[0]["companyID"].ToString(),dr);
                    }
                    else
                    {
                        limit = p_rules[0]["limit"].ToString();
                        key = string.Format(keyp, action.Rows[0]["conduitID"].ToString(), action.Rows[0]["productID"].ToString(), dr);
                    }
                    
                    if (string.IsNullOrEmpty(limit))
                        return info;

                    //string[] tempKey = { string.Format("{0}-9999", key), string.Format("{0}-999", key), key };

                    if (typeid == 0)//基本规则
                    {
                        JObject jinfo = JObject.Parse(limit);
                        string hhmm = DateTime.Now.ToString("HHmm");
                        int cptime = Convert.ToInt32(hhmm);

                        if (null != jinfo["stime"] && null != jinfo["etime"])
                        {
                            int st = Convert.ToInt32(jinfo["stime"].ToString().Replace(":", ""));
                            int et = Convert.ToInt32(jinfo["etime"].ToString().Replace(":", ""));

                            if (cptime >= st && cptime <= et)
                                return "{\"resultCode\":\"1\",\"status\":\"屏蔽时段\"}";
                        }

                        if (null != jinfo["empty"] && jinfo["empty"].ToString() == "true")
                        {
                            if (null == ht["mobile"].ToString() || string.IsNullOrEmpty(ht["mobile"].ToString()))
                                return "{\"resultCode\":\"1\",\"status\":\"请求失败\"}";
                        }

                        if (!string.IsNullOrEmpty(ht["mobile"].ToString()))
                        {
                            if (null != jinfo["blacklist"] && jinfo["blacklist"].ToString() == "true")
                            {
                                int bid = Service.getBlackList(ht["mobile"].ToString());
                                if (bid > 0)
                                    return "{\"resultCode\":\"1\",\"status\":\"黑名单用户\"}";
                            }
                            if (null != jinfo["disabled"] && !string.IsNullOrEmpty(jinfo["disabled"].ToString()))
                            {
                                string number = ht["mobile"].ToString().Substring(0, 3);
                                if (jinfo["disabled"].ToString().IndexOf(number) > -1)
                                    return "{\"resultCode\":\"1\",\"status\":\"屏蔽号段\"}";
                            }
                        }
                    }
                    else if (typeid == 1)//抓取规则
                    {
                            if (!string.IsNullOrEmpty(result))
                            {
                                string itemValue = null;
                                JArray array = JArray.Parse(limit);
                                bool isItem = wordDict.TryGetValue(key, out itemValue);

                                if (!string.IsNullOrEmpty(itemValue))
                                {
                                    if (key.IndexOf("9999") > -1)//通过IndexOf 来判断不同状态 不能能改变判断顺序
                                    {
                                        if (itemValue.IndexOf(date_m) > -1)
                                            return "{\"resultCode\":\"9999\",\"status\":\"本月通道量满\"}";
                                        else
                                        {
                                            JObject temp=JObject.Parse(itemValue);
                                            temp["datatime"]=date_m;
                                            wordDict.TryUpdate(key, temp.ToString(), itemValue);
                                        }
                                    }
                                    else if (key.IndexOf("999") > -1)
                                    {
                                        if (itemValue.IndexOf(datatime) > -1)
                                            return "{\"resultCode\":\"999\",\"status\":\"今日通道量满\"}";
                                        else
                                        {
                                            JObject temp = JObject.Parse(itemValue);
                                            temp["datatime"] = datatime;
                                            wordDict.TryUpdate(key, temp.ToString(), itemValue);
                                        }
                                    }
                                    else
                                    {
                                        if (string.IsNullOrEmpty(areainfo.Province))
                                            return null;
                                        JObject itemJson = JObject.Parse(itemValue);
                                        if (itemValue.IndexOf(datatime) > -1)
                                        {
                                            if (itemJson["area"].ToString().IndexOf(areainfo.Province) > -1)
                                                return "{\"resultCode\":\"99\",\"status\":\"" + areainfo.Province + "省份量满\"}";
                                        }
                                        else
                                        {
                                            itemJson["datatime"] = datatime;
                                            itemJson["area"] = areainfo.Province;
                                            wordDict.TryUpdate(key, itemJson.ToString(), itemValue);
                                        }
                                    } 
                                }
                                else
                                {
                                    foreach (JObject item in array)
                                    {
                                        if (result.IndexOf(item["word"].ToString()) > -1)
                                        {
                                            if (limit.IndexOf("9999") > -1)
                                            {
                                                item["datatime"] = date_m;
                                                wordDict.TryAdd(string.Format("{0}-9999", key), item.ToString().Replace(" ", ""));
                                                return "{\"resultCode\":\"9999\",\"status\":\"本月通道量满\"}";
                                            }
                                            else if (limit.IndexOf("999") > -1)
                                            {
                                                item["datatime"] = datatime;
                                                wordDict.TryAdd(string.Format("{0}-999", key), item.ToString().Replace(" ", ""));
                                                return "{\"resultCode\":\"999\",\"status\":\"今日通道量满\"}";
                                            }
                                            else 
                                            {
                                                item["datatime"] = datatime;
                                                item["area"] = areainfo.Province;
                                                wordDict.TryAdd(key, item.ToString().Replace(" ", ""));
                                                return "{\"resultCode\":\"99\",\"status\":\"" + areainfo.Province + "省份量满\"}";
                                            }
                                        }
                                    }
                                }
                            }
                    }
                    else if (typeid == 2)//读取省份到量规则
                    {
                        string itemValue = null;
                        bool isItem = areaDict.TryGetValue(key, out itemValue);
                        if (!string.IsNullOrEmpty(itemValue))
                        {
                            if (itemValue.IndexOf(areainfo.Province) == -1)
                            {
                                if (itemValue.IndexOf(datatime) > -1)
                                    return "{\"resultCode\":\"99\",\"status\":\"" + areainfo.Province + "省份量满\"}";
                            }
                        }
                    }
                    else if (typeid == 3)//读取总量规则
                    {
                        string itemValue = null;
                        bool isItem = feeDict.TryGetValue(key, out itemValue);
                        if (!string.IsNullOrEmpty(itemValue))
                        {
                            if (itemValue.IndexOf("9999") > -1)
                                return "{\"resultCode\":\"9999\",\"status\":\"本月通道量满\"}";

                            else if (itemValue.IndexOf(datatime) > -1)
                                return "{\"resultCode\":\"999\",\"status\":\"今日通道量满\"}";
                        }
                        /*if (!string.IsNullOrEmpty(itemValue))
                        {
                            if (itemValue.IndexOf("9999") > -1)
                                return "{\"resultCode\":\"9999\",\"status\":\"本月通道量满\"}";

                            else if (itemValue.IndexOf(datatime) > -1)
                                return "{\"resultCode\":\"999\",\"status\":\"今日通道量满\"}";
                            else
                            {
                                JObject json = JObject.Parse(itemValue);
                                json["datatime"] = datatime;
                                bool returnTrue = feeDict.TryUpdate(key, json.ToString().Replace(" ", ""), itemValue);
                            }
                        }
                        else
                        {
                            JObject json = JObject.Parse(limit);
                            string datainfo = Service.getLimitTotal(Convert.ToInt32(action.Rows[0]["conduitID"]), Convert.ToInt32(action.Rows[0]["productID"]), 0, null, null, json["field"].ToString(), Convert.ToBoolean(json["filter"]), tablename);
                            if (!string.IsNullOrEmpty(datainfo))
                            {
                                JObject temp = JObject.Parse(datainfo);

                                if (null != json["month"])
                                {
                                    json.Add(new JProperty("datatime", date_m));
                                    feeDict.TryAdd(key + "-9999", json.ToString().Replace(" ", ""));
                                    return "{\"resultCode\":\"9999\",\"status\":\"本月通道量满\"}";
                                }
                                else
                                {
                                    json.Add(new JProperty("datatime", datatime));
                                    feeDict.TryAdd(key + "-999", json.ToString().Replace(" ", ""));
                                    return "{\"resultCode\":\"999\",\"status\":\"今日通道量满\"}";
                                }
                            }
                        }*/
                    }
                    else if (typeid == 5)//请求日月限
                    {

                        JObject json = JObject.Parse(limit);
                        int companyID = 0;
                        int fee = 0;
                        if (json["field"].ToString() == "companyid")
                            companyID = Convert.ToInt32(action.Rows[0]["productID"]);
                        else if (json["field"].ToString() == "fee")
                        {
                            companyID = Convert.ToInt32(action.Rows[0]["productID"]);
                            fee = Convert.ToInt32(action.Rows[0]["fee"]);
                        }
                       

                        DataTable datainfo = Service.getLimitTotal(Convert.ToInt32(action.Rows[0]["conduitID"]), Convert.ToInt32(action.Rows[0]["productID"]), companyID, null, null, fee, null, json["field"].ToString(), Convert.ToBoolean(json["filter"]), tablename);
                        if (datainfo.Rows.Count > 0)
                        {
                            if (null != json["month"])
                            {
                                if (Convert.ToInt32(datainfo.Rows[0]["number"]) >= Convert.ToInt32(json["month"]))
                                    return "{\"resultCode\":\"1\",\"status\":\"提交月限\"}";
                            }
                            else
                            {
                                if (Convert.ToInt32(datainfo.Rows[0]["number"]) >= Convert.ToInt32(json["day"]))
                                    return "{\"resultCode\":\"1\",\"status\":\"号码日限\"}";
                            }
                        }
                    }
                }
            }
            return info;
        }


        /// <summary>
        /// 删除内存规则
        /// </summary>
        /// <param name="key"></param>
        /// <param name="dic"></param>
        /// <param name="area"></param>
        /// <returns></returns>
        public static bool removeRules(string key, ConcurrentDictionary<string, string> dic, string province)
        {
            bool returnTrue = false;
            if (!string.IsNullOrEmpty(key))
            {
                if (!string.IsNullOrEmpty(province))
                {
                     string itemValue = null;
                     string removedItem=null;
                     bool isItem = dic.TryGetValue(key, out itemValue);
                     if (!string.IsNullOrEmpty(itemValue))
                     {
                         JObject json = JObject.Parse(itemValue);
                         if (null != json["area"])
                         {
                             if (json["area"].ToString().IndexOf(province) > -1)
                             {
                                 json["area"] = json["area"].ToString().Replace(province, "").Replace(",,", ",");
                                 returnTrue = dic.TryUpdate(key, json.ToString().Replace(" ", ""), itemValue);
                             }
                         }
                         else
                           returnTrue = dic.TryRemove(key, out removedItem);
                         
                     }
                }
            }
            return returnTrue;
        }


       public static string getRulesInfo(DataTable dt, Hashtable ht, DataTable action, string result, string tablename)
        {
            string date = DateTime.Now.ToString("yyyy-MM-dd");
            string stime = date + " 00:00";
            string etime = date + " 23:59";
            string info = null;
            string month = string.Empty;
           
            mobileArea areainfo = new mobileArea();

            if (Convert.ToInt32(action.Rows[0]["startFlag"]) == 0)
                return "{\"resultCode\":\"-1\",\"status\":\"通道未开通!\"}";

            if (Convert.ToInt32(action.Rows[0]["syncStart"]) == 0)
                return "{\"resultCode\":\"-1\",\"status\":\"通道未开通!\"}";

            if (null != ht["Mobile"] && !string.IsNullOrEmpty(ht["Mobile"].ToString()))
            {
                areainfo = Service.getMobileArea(ht["Mobile"].ToString());
                if (string.IsNullOrEmpty(areainfo.Province))
                    return info;
            }

            string mailinfo = dataManage.getDictionaryItem("邮箱配置");
            if (string.IsNullOrEmpty(mailinfo))
                return info;

            JObject setmail = JObject.Parse(mailinfo);
            Email.mailFrom = setmail["from"].ToString();
            Email.mailPwd = setmail["password"].ToString();
            Email.host = setmail["smtp"].ToString();
            Email.port = Convert.ToInt32(setmail["port"]);
            Email.mailSubject = setmail["subject"].ToString();
            Email.isbodyHtml = true;
            if (setmail["html"].ToString() == "false")
                Email.isbodyHtml = false;


            DataTable rules = Service.getJsonRules(Convert.ToInt32(action.Rows[0]["productid"]));
            DataTable group = Service.getRulesGroup(Convert.ToInt32(action.Rows[0]["productid"]));
            if (group.Rows.Count > 0)
            {
                foreach (DataRow dr in group.Rows)
                {
                    string limit=null;
                    DataRow[] a_rules = rules.Select("typeid=" + dr["typeid"].ToString() + " and actionid=" + action.Rows[0]["infoid"].ToString());
                    DataRow[] c_rules = rules.Select("typeid=" + dr["typeid"].ToString() + " and companyid=" + action.Rows[0]["companyID"].ToString());
                    DataRow[] p_rules = rules.Select("typeid=" + dr["typeid"].ToString());

                    if (a_rules.Length > 0)
                        limit = a_rules[0]["limit"].ToString();
                    else if (c_rules.Length > 0)
                        limit = c_rules[0]["limit"].ToString();
                    else
                        limit = p_rules[0]["limit"].ToString();

                    //if (!string.IsNullOrEmpty(limit))
                     // return resultRuleInfo(Convert.ToInt32(action.Rows[0]["conduitID"]), Convert.ToInt32(action.Rows[0]["productID"]), Convert.ToInt32(dr["typeid"]), ht["Mobile"].ToString(), limit);
                }
            }
           




            /*foreach (DataRow dr in rules.Rows)
            {
                int typeid = Convert.ToInt32(dr["typeid"]);
                switch (typeid)
                {
                    case 0:
                           if (!string.IsNullOrEmpty(dr["limit"].ToString()))
                            {
                                JObject jinfo = JObject.Parse(dr["limit"].ToString());
                                string hhmm = DateTime.Now.ToString("HHmm");
                                int cptime = Convert.ToInt32(hhmm);
                                if (null != jinfo["stime"] && null != jinfo["etime"])
                                {
                                    int st = Convert.ToInt32(jinfo["stime"].ToString().Replace(":", ""));
                                    int et = Convert.ToInt32(jinfo["etime"].ToString().Replace(":", ""));

                                    if (cptime >=st  && cptime <= et)
                                       return "{\"resultCode\":\"1\",\"status\":\"屏蔽时段\"}";
                                    
                                }
                                if (null != jinfo["empty"] && jinfo["empty"].ToString()=="true")
                                {
                                    if(null==ht["Mobile"] || string.IsNullOrEmpty(ht["Mobile"].ToString()))
                                        return "{\"resultCode\":\"1\",\"status\":\"请求失败\"}";
                                }
                                if (!string.IsNullOrEmpty(ht["Mobile"].ToString()))
                                {
                                    if (null != jinfo["blacklist"] && jinfo["blacklist"].ToString() == "true")
                                    {
                                        int bid = Service.getBlackList(ht["Mobile"].ToString());
                                        if (bid > 0)
                                            return "{\"resultCode\":\"1\",\"status\":\"黑名单用户\"}";
                                    }
                                    if (null != jinfo["disabled"] && !string.IsNullOrEmpty(jinfo["disabled"].ToString()))
                                    {
                                        string number = ht["Mobile"].ToString().Substring(0,3);
                                        if(jinfo["disabled"].ToString().IndexOf(number)>-1)
                                            return "{\"resultCode\":\"1\",\"status\":\"屏蔽号段\"}";
                                    }
                                }
                           }
                           break;
                    //到量抓取策略
                    case 1:
                        if (string.IsNullOrEmpty(result))
                            return info;
                        if (!string.IsNullOrEmpty(dr["limit"].ToString()))
                        {
                            int send=-1;
                            JArray limtinfo = JArray.Parse(dr["limit"].ToString());
                            int line = getWordMtable(Convert.ToInt32(dt.Rows[0]["conduitid"]), Convert.ToInt32(action.Rows[0]["productID"]), limtinfo, areainfo.Province, date, result);
                            JObject item = (JObject)limtinfo[line];
                            if (null != limtinfo[line]["area"])
                                send = getMailTab(wordMailTab, Convert.ToInt32(dt.Rows[0]["conduitid"]), Convert.ToInt32(action.Rows[0]["productID"]), item, areainfo.Province, date);
                            else
                                send = getMailTab(wordMailTab, Convert.ToInt32(dt.Rows[0]["conduitid"]), Convert.ToInt32(action.Rows[0]["productID"]), item, null, date);
                            
                            if (send == 0)
                            {
                                setMailTab(wordMailTab, Convert.ToInt32(dt.Rows[0]["conduitid"]), Convert.ToInt32(action.Rows[0]["productID"]), limtinfo, areainfo.Province, result);
                                string cp = Service.getLimitCompany(Convert.ToInt32(dt.Rows[0]["conduitid"]), Convert.ToInt32(action.Rows[0]["productID"]), stime, etime, dt.Rows[0]["tablename"].ToString());
                                if (!string.IsNullOrEmpty(dt.Rows[0]["notifyMail"].ToString()))
                                {
                                    Email.mailToArray = dt.Rows[0]["notifyMail"].ToString().Split(',');
                                    Email.mailBody = "<html><body><span style='color:#ff9900'>" + date + "<br>" + action.Rows[0]["conduitname"].ToString() + "-" + action.Rows[0]["productname"].ToString() + "-" + month + "通道量满<br>" + cp + "</span></body></html>";
                                    Email.Send();
                                }
                                //return "{\"resultCode\":\"99\",\"StreamingNo\":\"\",\"status\":\"" + message + "\"}";
                            }
                            //else if(send>0)
                                //return "{\"resultCode\":\"99\",\"StreamingNo\":\"\",\"status\":\"" + message + "\"}";
                        }
                        break;
                    //单省限制策略
                    case 2:
                        if (!string.IsNullOrEmpty(dr["limit"].ToString()))
                        {
                            JObject jinfo = JObject.Parse(dr["limit"].ToString());
                            int send2 = getAreaMtable(Convert.ToInt32(dt.Rows[0]["conduitid"]), Convert.ToInt32(action.Rows[0]["productID"]), jinfo, areainfo.Province, date);
                            if (send2 == 0)
                            {
                                string data = Service.LimitRules(dr["limit"].ToString(), areainfo.Province, Convert.ToInt32(action.Rows[0]["conduitID"]), 0, Convert.ToInt32(action.Rows[0]["productID"]), null, null, "public_notify_2017");
                                if (!string.IsNullOrEmpty(data))
                                {
                                    JObject limtinfo = JObject.Parse(data);
                                    if (limtinfo["flag"].ToString().IndexOf('m') > -1)
                                        month = DateTime.Now.Month.ToString() + "月";
                                    //setMailTab(areaMailTab, Convert.ToInt32(dt.Rows[0]["conduitid"]), Convert.ToInt32(action.Rows[0]["productID"]), limtinfo, areainfo.Province);
                                    string cp = Service.getLimitCompany(Convert.ToInt32(dt.Rows[0]["conduitid"]), Convert.ToInt32(action.Rows[0]["productID"]), stime, etime, dt.Rows[0]["tablename"].ToString());
                                    if (!string.IsNullOrEmpty(dt.Rows[0]["notifyMail"].ToString()))
                                    {
                                        Email.mailToArray = dt.Rows[0]["notifyMail"].ToString().Split(',');
                                        Email.mailBody = "<html><body><span style='color:#ff9900'>" + date + "<br>" + action.Rows[0]["conduitname"].ToString() + "-" + action.Rows[0]["productname"].ToString() + "-" + areainfo.Province + month + "量满<br>" + cp + "</span></body></html>";
                                        Email.Send();
                                    }
                                    return "{\"resultCode\":\"99\",\"StreamingNo\":\"\",\"status\":\"" + areainfo.Province + month + "量满\"}";
                                }
                            }
                            else
                                return "{\"resultCode\":\"99\",\"StreamingNo\":\"\",\"status\":\"" + areainfo.Province + "量满\"}";
                            
                        }
                        break;
                    //总量限制策略
                    case 3:
                          if(!string.IsNullOrEmpty(dr["limit"].ToString()))
                          {
                              JObject jinfo = JObject.Parse(dr["limit"].ToString());
                              int send3 = getFeeMtable(Convert.ToInt32(dt.Rows[0]["conduitid"]), Convert.ToInt32(action.Rows[0]["productID"]), jinfo, null, date);
                              if (send3 == 0)
                              {
                                  string data = Service.LimitRules(dr["limit"].ToString(), null, Convert.ToInt32(dt.Rows[0]["conduitid"]), 0, Convert.ToInt32(action.Rows[0]["productID"]), null, null, "public_notify_2017");
                                  if (!string.IsNullOrEmpty(data))
                                  {
                                      JObject limtinfo = JObject.Parse(data);
                                      if (limtinfo["flag"].ToString().IndexOf('m') > -1)
                                          month = DateTime.Now.Month.ToString() + "月";
                                      //setMailTab(feeMailTab, Convert.ToInt32(dt.Rows[0]["conduitid"]), Convert.ToInt32(action.Rows[0]["productID"]), limtinfo, null);
                                      string cp = Service.getLimitCompany(Convert.ToInt32(dt.Rows[0]["conduitid"]), Convert.ToInt32(action.Rows[0]["productID"]), stime, etime, dt.Rows[0]["tablename"].ToString());
                                      if (!string.IsNullOrEmpty(dt.Rows[0]["notifyMail"].ToString()))
                                      {
                                          Email.mailToArray = dt.Rows[0]["notifyMail"].ToString().Split(',');
                                          Email.mailBody = "<html><body><span style='color:#ff9900'>" + date + "<br>" + action.Rows[0]["conduitname"].ToString() + "-" + action.Rows[0]["productname"].ToString() + "-" + month + "通道量满<br>" + cp + "</span></body></html>";
                                          Email.Send();
                                      }
                                      return "{\"resultCode\":\"99\",\"StreamingNo\":\"\",\"status\":\"" + month + "通道量满\"}";
                                  }
                              }
                              else
                                  return "{\"resultCode\":\"99\",\"StreamingNo\":\"\",\"status\":\"通道量满\"}";
                              
                        } break;
                    //点播日限月限策略
                    case 4:
                        if (!string.IsNullOrEmpty(dr["limit"].ToString()))
                            {
                                JArray json = JArray.Parse(dr["limit"].ToString());

                                string feeinfo = Service.searchRecordFee(json["field"].ToString(), Convert.ToInt32(dt.Rows[0]["conduitid"]), Convert.ToInt32(action.Rows[0]["productID"]), Convert.ToInt32(action.Rows[0]["companyID"]), dt.Rows[0]["tablename"].ToString(), ht[json["field"].ToString()].ToString());

                                if (!string.IsNullOrEmpty(feeinfo))
                                {
                                    JObject jsonfee = JObject.Parse(feeinfo);
                                    int dayfee = Convert.ToInt32(jsonfee["dayfee"]);
                                    int monthfee = Convert.ToInt32(jsonfee["monthfee"]);

                                    foreach (JObject dataitem in json)
                                    {
                                        if (null != dataitem["dayfee"])
                                        {
                                            int fee = Convert.ToInt32(dataitem["dayfee"]);
                                            if (fee >= dayfee)
                                                return "{\"resultCode\":\"1\",\"StreamingNo\":\"\",\"status\":\"" + dataitem["message"].ToString() + "\"}";
                                        }
                                        else if (null != dataitem["monthfee"])
                                        {
                                            int fee = Convert.ToInt32(dataitem["monthfee"]);
                                            if (fee >= monthfee)
                                                return "{\"resultCode\":\"1\",\"StreamingNo\":\"\",\"status\":\"" + dataitem["message"].ToString() + "\"}";
                                        }
                                    }
                               }
                            
                        } break;
                    //包月请求策略
                    case 5:
                        if (!string.IsNullOrEmpty(dr["limit"].ToString()))
                            {
                                JObject json = JObject.Parse(dr["limit"].ToString());
                                int setdrequest = Convert.ToInt32(json["day"]);
                                int setmrequest = Convert.ToInt32(json["month"]);

                                string requestinfo = Service.searchRecordCount(json["field"].ToString(), Convert.ToInt32(dt.Rows[0]["conduitid"]), Convert.ToInt32(action.Rows[0]["productID"]), Convert.ToInt32(ht["companyID"]), dt.Rows[0]["tablename"].ToString(), ht[json["field"].ToString()].ToString());
                                if (!string.IsNullOrEmpty(requestinfo))
                                {
                                    JObject requestjson = JObject.Parse(requestinfo);
                                    int getdrequest = Convert.ToInt32(requestjson["day"]);
                                    int getmrequest = Convert.ToInt32(requestjson["month"]);
                                    if (getdrequest > 0 && getdrequest >= setdrequest)
                                        return "{\"resultCode\":\"1\",\"StreamingNo\":\"\",\"status\":\"" + json["message"].ToString() + "\"}";
                                    if (getmrequest > 0 && getmrequest >= setmrequest)
                                        return "{\"resultCode\":\"1\",\"StreamingNo\":\"\",\"status\":\"" + json["message"].ToString() + "\"}";
                                }
                         } break;
                   
                    //省份判断
                    case 6:
                                string limit = null;
                                DataRow[] rows = rules.Select("typeid=6 and actionid=" + action.Rows[0]["infoid"].ToString());
                                if (rows.Length > 0)
                                    limit = rows[0]["limit"].ToString();
                                else
                                    limit = dr["limit"].ToString();

                                if (!string.IsNullOrEmpty(limit))
                                {
                                    JObject jlimit = JObject.Parse(limit);
                                    string enable = jlimit["enable"].ToString();
                                   
                                    if (string.IsNullOrEmpty(areainfo.Province) && jlimit["get"].ToString() == "true")
                                    {
                                        string geturl = dataManage.getDictionary(35);
                                        JObject urljson = JObject.Parse(geturl);
                                        JArray urlItem = JArray.Parse(urljson["data"].ToString());
                                        foreach (JObject items in urlItem)
                                        {
                                            string[] pathinfo = items["value"].ToString().Split('?');
                                            string param = pathinfo[1].Replace("#mobile", ht["Mobile"].ToString());
                                            string getstr = Utils.HttpGet(pathinfo[0], param, 2000);
                                            getstr = getstr.Replace("querycallback(", "").Replace(")", "");
                                            JObject area = JObject.Parse(getstr);
                                            if (!string.IsNullOrEmpty(area["Province"].ToString()))
                                            {
                                                string getarea = area["Province"].ToString();
                                                if (enable.IndexOf(getarea) == -1)
                                                    return "{\"resultCode\":\"-1\",\"StreamingNo\":\"\",\"status\":\"" + getarea + jlimit["message"].ToString() + "\"}";
                                            }
                                        }
                                    }

                                   if (enable.IndexOf(areainfo.Province) == -1)
                                      return "{\"resultCode\":\"-1\",\"status\":\"" + areainfo.Province + jlimit["message"].ToString() + "\"}";
                                }
                               break;
                    default:
                        break;
                }
            }*/

            return info;
        } 

        

    }//
}
