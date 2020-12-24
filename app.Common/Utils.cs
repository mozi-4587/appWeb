using System;
using System.Collections.Generic;
using System.Text;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using System.Net;
using Microsoft.VisualBasic;
using System.Web;
using System.IO;
using System.Xml;
using System.Net.Mail;
using System.Collections;
using Newtonsoft.Json.Linq;


namespace app.Common
{
    public class Utils
    {
        private static Regex RegexBr = new Regex(@"(\r\n)", RegexOptions.IgnoreCase);

        public static Hashtable mailTab = Hashtable.Synchronized(new Hashtable());
        public static Hashtable feeMailTab = Hashtable.Synchronized(new Hashtable());
        public static Hashtable areaMailTab = Hashtable.Synchronized(new Hashtable());
        public static Hashtable wordMailTab = Hashtable.Synchronized(new Hashtable());
        public static Hashtable tempMailTab = Hashtable.Synchronized(new Hashtable());

      
        /// <summary>
        /// 删除字符串尾部的回车/换行/空格
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static string RTrim(string str)
        {
            for (int i = str.Length; i >= 0; i--)
            {
                if (str[i].Equals(" ") || str[i].Equals("\r") || str[i].Equals("\n"))
                {
                    str.Remove(i, 1);
                }
            }
            return str;
        }

        /// <summary>
        /// 删除字符串的回车/换行/空格
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static string Trim(string str)
        {
            for (int i = 0; i < str.Length; i++)
            {
                if (str[i].Equals(" ") || str[i].Equals("\r") || str[i].Equals("\n"))
                {
                    str.Remove(i, 1);
                }
            }
            return str;
           
     
        }

        /// <summary>
        /// 清除给定字符串中的回车及换行符
        /// </summary>
        /// <param name="str">要清除的字符串</param>
        /// <returns>清除后返回的字符串</returns>
        public static string ClearBR(string str)
        {
            //Regex r = null;
            Match m = null;

            for (m = RegexBr.Match(str); m.Success; m = m.NextMatch())
            {
                str = str.Replace(m.Groups[0].ToString(), "");
            }


            return Regex.Replace(str, "\\s{2,}", "");
        }

        /// <summary>
        /// 字符串如果操过指定长度则将超出的部分用指定字符串代替
        /// </summary>
        /// <param name="p_SrcString">要检查的字符串</param>
        /// <param name="p_Length">指定长度</param>
        /// <param name="p_TailString">用于替换的字符串</param>
        /// <returns>截取后的字符串</returns>
        public static string GetSubString(string p_SrcString, int p_Length, string p_TailString)
        {
            return GetSubString(p_SrcString, 0, p_Length, p_TailString);
        }


        /// <summary>
        /// 取指定长度的字符串
        /// </summary>
        /// <param name="p_SrcString">要检查的字符串</param>
        /// <param name="p_StartIndex">起始位置</param>
        /// <param name="p_Length">指定长度</param>
        /// <param name="p_TailString">用于替换的字符串</param>
        /// <returns>截取后的字符串</returns>
        public static string GetSubString(string p_SrcString, int p_StartIndex, int p_Length, string p_TailString)
        {


            string myResult = p_SrcString;

            //当是日文或韩文时(注:中文的范围:\u4e00 - \u9fa5, 日文在\u0800 - \u4e00, 韩文为\xAC00-\xD7A3)
            if (System.Text.RegularExpressions.Regex.IsMatch(p_SrcString, "[\u0800-\u4e00]+") ||
                System.Text.RegularExpressions.Regex.IsMatch(p_SrcString, "[\xAC00-\xD7A3]+"))
            {
                //当截取的起始位置超出字段串长度时
                if (p_StartIndex >= p_SrcString.Length)
                {
                    return "";
                }
                else
                {
                    return p_SrcString.Substring(p_StartIndex,
                                                   ((p_Length + p_StartIndex) > p_SrcString.Length) ? (p_SrcString.Length - p_StartIndex) : p_Length);
                }
            }


            if (p_Length >= 0)
            {
                byte[] bsSrcString = Encoding.Default.GetBytes(p_SrcString);

                //当字符串长度大于起始位置
                if (bsSrcString.Length > p_StartIndex)
                {
                    int p_EndIndex = bsSrcString.Length;

                    //当要截取的长度在字符串的有效长度范围内
                    if (bsSrcString.Length > (p_StartIndex + p_Length))
                    {
                        p_EndIndex = p_Length + p_StartIndex;
                    }
                    else
                    {   //当不在有效范围内时,只取到字符串的结尾

                        p_Length = bsSrcString.Length - p_StartIndex;
                        p_TailString = "";
                    }



                    int nRealLength = p_Length;
                    int[] anResultFlag = new int[p_Length];
                    byte[] bsResult = null;

                    int nFlag = 0;
                    for (int i = p_StartIndex; i < p_EndIndex; i++)
                    {

                        if (bsSrcString[i] > 127)
                        {
                            nFlag++;
                            if (nFlag == 3)
                            {
                                nFlag = 1;
                            }
                        }
                        else
                        {
                            nFlag = 0;
                        }

                        anResultFlag[i] = nFlag;
                    }

                    if ((bsSrcString[p_EndIndex - 1] > 127) && (anResultFlag[p_Length - 1] == 1))
                    {
                        nRealLength = p_Length + 1;
                    }

                    bsResult = new byte[nRealLength];

                    Array.Copy(bsSrcString, p_StartIndex, bsResult, 0, nRealLength);

                    myResult = Encoding.Default.GetString(bsResult);

                    myResult = myResult + p_TailString;
                }
            }

            return myResult;
        }

        /// <summary>
        /// 从字符串的指定位置截取指定长度的子字符串
        /// </summary>
        /// <param name="str">原字符串</param>
        /// <param name="startIndex">子字符串的起始位置</param>
        /// <param name="length">子字符串的长度</param>
        /// <returns>子字符串</returns>
        public static string CutString(string str, int startIndex, int length)
        {
            if (startIndex >= 0)
            {
                if (length < 0)
                {
                    length = length * -1;
                    if (startIndex - length < 0)
                    {
                        length = startIndex;
                        startIndex = 0;
                    }
                    else
                    {
                        startIndex = startIndex - length;
                    }
                }


                if (startIndex > str.Length)
                {
                    return "";
                }


            }
            else
            {
                if (length < 0)
                {
                    return "";
                }
                else
                {
                    if (length + startIndex > 0)
                    {
                        length = length + startIndex;
                        startIndex = 0;
                    }
                    else
                    {
                        return "";
                    }
                }
            }

            if (str.Length - startIndex < length)
            {
                length = str.Length - startIndex;
            }

            return str.Substring(startIndex, length);
        }


        /// <summary>
        /// int型转换为string型
        /// </summary>
        /// <returns>转换后的string类型结果</returns>
        public static string IntToStr(int intValue)
        {
            //
            return Convert.ToString(intValue);
        }
        /// <summary>
        /// MD5函数
        /// </summary>
        /// <param name="str">原始字符串</param>
        /// <returns>MD5结果</returns>
        public static string MD5(string str)
        {
            byte[] b = Encoding.Default.GetBytes(str);
            b = new MD5CryptoServiceProvider().ComputeHash(b);
            string ret = "";
            for (int i = 0; i < b.Length; i++)
                ret += b[i].ToString("x").PadLeft(2, '0');
            return ret;
        }

        public static string MD5(string str, System.Text.Encoding code)
        {
            byte[] b = code.GetBytes(str);
            b = new MD5CryptoServiceProvider().ComputeHash(b);
            string ret = "";
            ret=BitConverter.ToString(b).Replace("-", "");
            return ret;
        }

        /// <summary>
        /// SHA256函数
        /// </summary>
        /// /// <param name="str">原始字符串</param>
        /// <returns>SHA256结果</returns>
        public static string SHA256(string str)
        {
            byte[] SHA256Data = Encoding.UTF8.GetBytes(str);
            SHA256Managed Sha256 = new SHA256Managed();
            byte[] Result = Sha256.ComputeHash(SHA256Data);
            return Convert.ToBase64String(Result);  //返回长度为44字节的字符串
        }


        public static string XmlToJson(string xml)
        {
            XmlDocument doc = new XmlDocument();
            doc.LoadXml(xml);
            string json = Newtonsoft.Json.JsonConvert.SerializeXmlNode(doc);

            return json;
        }


        /// <summary>
        /// 判断指定字符串在指定字符串数组中的位置
        /// </summary>
        /// <param name="strSearch">字符串</param>
        /// <param name="stringArray">字符串数组</param>
        /// <param name="caseInsensetive">是否不区分大小写, true为不区分, false为区分</param>
        /// <returns>字符串在指定字符串数组中的位置, 如不存在则返回-1</returns>
        public static int GetInArrayID(string strSearch, string[] stringArray, bool caseInsensetive)
        {
            for (int i = 0; i < stringArray.Length; i++)
            {
                if (caseInsensetive)
                {
                    if (strSearch.ToLower() == stringArray[i].ToLower())
                    {
                        return i;
                    }
                }
                else
                {
                    if (strSearch == stringArray[i])
                    {
                        return i;
                    }
                }

            }
            return -1;
        }


        /// <summary>
        /// 格式化字节数字符串
        /// </summary>
        /// <param name="bytes"></param>
        /// <returns></returns>
        public static string FormatBytesStr(int bytes)
        {
            if (bytes > 1073741824)
            {
                return ((double)(bytes / 1073741824)).ToString("0") + "G";
            }
            if (bytes > 1048576)
            {
                return ((double)(bytes / 1048576)).ToString("0") + "M";
            }
            if (bytes > 1024)
            {
                return ((double)(bytes / 1024)).ToString("0") + "K";
            }
            return bytes.ToString() + "Bytes";
        }

        /// <summary>
        /// 判断字符串是否是yy-mm-dd字符串
        /// </summary>
        /// <param name="str">待判断字符串</param>
        /// <returns>判断结果</returns>
        public static bool IsDateString(string str)
        {
            return Regex.IsMatch(str, @"(\d{4})-(\d{1,2})-(\d{1,2})");
        }


        /// <summary>
        /// 转换为简体中文
        /// </summary>
        public static string ToSChinese(string str)
        {
            return Strings.StrConv(str, VbStrConv.SimplifiedChinese, 0);
        }

        /// <summary>
        /// 转换为繁体中文
        /// </summary>
        public static string ToTChinese(string str)
        {
            return Strings.StrConv(str, VbStrConv.TraditionalChinese, 0);
        }

        /// <summary>
        /// 分割字符串
        /// </summary>
        public static string[] SplitString(string strContent, string strSplit)
        {
            if (strContent.IndexOf(strSplit) < 0)
            {
                string[] tmp = { strContent };
                return tmp;
            }
            return Regex.Split(strContent, Regex.Escape(strSplit), RegexOptions.IgnoreCase);
        }

        /// <summary>
        /// 分割字符串
        /// </summary>
        /// <returns></returns>
        public static string[] SplitString(string strContent, string strSplit, int p_3)
        {
            string[] result = new string[p_3];

            string[] splited = SplitString(strContent, strSplit);

            for (int i = 0; i < p_3; i++)
            {
                if (i < splited.Length)
                    result[i] = splited[i];
                else
                    result[i] = string.Empty;
            }

            return result;
        }

        /// <summary>
        /// 将字符串转换为16进制
        /// </summary>
        /// <param name="strEncode"></param>
        public static string StrinToX(string strEncode)
        {
            string strReturn = "";//  存储转换后的编码
            foreach (short shortx in strEncode.ToCharArray())
            {
                strReturn += shortx.ToString("X4") + ",";
            }
            return CutString(strReturn, 0, strReturn.Length - 1);

        }

        /// <summary>
        /// 替换html字符
        /// </summary>
        public static string EncodeHtml(string strHtml)
        {
            if (strHtml != "")
            {
                strHtml = strHtml.Replace(",", "&def");
                strHtml = strHtml.Replace("'", "&dot");
                strHtml = strHtml.Replace(";", "&dec");
                strHtml = strHtml.Replace("<", "");
                strHtml = strHtml.Replace(">", "");
                strHtml = strHtml.Replace("%", "");
                //strHtml = strHtml.Replace("'", "");
                strHtml = strHtml.Replace("and", "");
                strHtml = strHtml.Replace("select", "");
                strHtml = strHtml.Replace("like", "");
                strHtml = strHtml.Replace("javascript", "");
                strHtml = strHtml.Replace("delete", "");
                strHtml = strHtml.Replace("insert", "");
                strHtml = strHtml.Replace("creat", "");
                return strHtml;
            }
            return "";
        }

        /// <summary>
        /// 随机加法
        /// </summary>
        /// <returns></returns>
        public static string CodeString()
        {
            char[] chars = "1234567890".ToCharArray();
            System.Random random = new Random();

            string validateCode = string.Empty;
            for (int i = 0; i < 8; i++)
            {
                if (i == 0)
                    validateCode += chars[random.Next(0, chars.Length)].ToString() + "+";
                else
                    validateCode += chars[random.Next(0, chars.Length)].ToString();
            }

            return validateCode;
        }

        /// <summary>
        /// 获取网络数据
        /// </summary>
        /// <param name="Method">类型 post or get</param>
        /// <param name="UrlStr">url</param>
        /// <param name="PostParam">post 参数 如aparam=x&bparam=y...</param>
        /// <returns></returns>
        public static string GetService(string Method, string UrlStr, string PostParam)
        {
            string result = string.Empty;

            if (Method.ToLower().Equals("post"))
            {
                HttpWebRequest hwrq = (HttpWebRequest)WebRequest.Create(UrlStr + "?" + PostParam);
                //下面是相关数据头和数据发送方法
                hwrq.Accept = "application/x-shockwave-flash, image/gif, image/x-xbitmap, image/jpeg, image/pjpeg, application/vnd.ms-excel, application/vnd.ms-powerpoint, application/msword, */*";
                hwrq.Referer = UrlStr + "?" + PostParam;
                hwrq.ContentType = "application/x-www-form-urlencoded";
                hwrq.UserAgent = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727; MAXTHON 2.0)";
                hwrq.KeepAlive = true;
                hwrq.Method = Method;

                //使用MiniSniffer来抓包分析应该发送什么数据
                ASCIIEncoding ASC2E = new ASCIIEncoding();
                byte[] bytePost = ASC2E.GetBytes(PostParam);
                hwrq.ContentLength = bytePost.Length;

                //下面是发送数据的字节流
                System.IO.Stream MyStream = hwrq.GetRequestStream();
                MyStream.Write(bytePost, 0, bytePost.Length);
                MyStream.Close();


                //创建HttpWebResponse实例
                HttpWebResponse hwrp = (HttpWebResponse)hwrq.GetResponse();
                System.IO.StreamReader MyStreamR = new System.IO.StreamReader(hwrp.GetResponseStream(), Encoding.UTF8);

                result = MyStreamR.ReadToEnd();
            }
            else if (Method.ToLower().Equals("get"))
            {
                WebClient myclien = new WebClient();
                System.IO.StreamReader mystream = new System.IO.StreamReader(myclien.OpenRead(UrlStr + "?" + PostParam), Encoding.UTF8);
                result = mystream.ReadToEnd().ToString().Trim();
            }

            return result;
        }


        public static string GetService(string Method, string UrlStr, string PostParam, int timeout)
        {
                string result = string.Empty;
                try
                {
                    if (Method.ToLower().Equals("post"))
                    {
                        HttpWebRequest hwrq = (HttpWebRequest)WebRequest.Create(UrlStr + "?" + PostParam);
                        //下面是相关数据头和数据发送方法
                        hwrq.Accept = "application/x-shockwave-flash, image/gif, image/x-xbitmap, image/jpeg, image/pjpeg, application/vnd.ms-excel, application/vnd.ms-powerpoint, application/msword, */*";
                        hwrq.Referer = UrlStr + "?" + PostParam;
                        hwrq.ContentType = "application/x-www-form-urlencoded";
                        hwrq.UserAgent = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727; MAXTHON 2.0)";
                        hwrq.KeepAlive = true;
                        hwrq.Method = Method;
                        if (timeout > 0)
                            hwrq.Timeout = timeout;
                        //使用MiniSniffer来抓包分析应该发送什么数据
                        ASCIIEncoding ASC2E = new ASCIIEncoding();
                        byte[] bytePost = ASC2E.GetBytes(PostParam);
                        hwrq.ContentLength = bytePost.Length;

                        //下面是发送数据的字节流
                        System.IO.Stream MyStream = hwrq.GetRequestStream();
                        MyStream.Write(bytePost, 0, bytePost.Length);
                        MyStream.Close();


                        //创建HttpWebResponse实例
                        HttpWebResponse hwrp = (HttpWebResponse)hwrq.GetResponse();
                        System.IO.StreamReader MyStreamR = new System.IO.StreamReader(hwrp.GetResponseStream(), Encoding.UTF8);

                        result = MyStreamR.ReadToEnd();
                    }
                    else
                        result=HttpGet(UrlStr, PostParam, timeout);

                }
                catch (Exception e)
                {
                    return "error";
                }

                return result;
        }


        public static string PostInfo(string Url, string Pramers)
        {
            string requestUriString = Url + "?" + Pramers;
            HttpWebRequest httpWebRequest = (HttpWebRequest)WebRequest.Create(requestUriString);
            httpWebRequest.Accept = "application/x-shockwave-flash, image/gif, image/x-xbitmap, image/jpeg, image/pjpeg, application/vnd.ms-excel, application/vnd.ms-powerpoint, application/msword, */*";
            httpWebRequest.Referer = requestUriString;
            httpWebRequest.ContentType = "application/x-www-form-urlencoded";
            httpWebRequest.UserAgent = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727; MAXTHON 2.0)";
            httpWebRequest.KeepAlive = true;
            httpWebRequest.Method = "POST";
            byte[] bytes = new ASCIIEncoding().GetBytes(Pramers);
            httpWebRequest.ContentLength = (long)bytes.Length;
            Stream requestStream = httpWebRequest.GetRequestStream();
            requestStream.Write(bytes, 0, bytes.Length);
            requestStream.Close();
            return new StreamReader(httpWebRequest.GetResponse().GetResponseStream(), Encoding.GetEncoding("utf-8")).ReadToEnd();
        }

    public static string HttpGet(string Url, string postDataStr)
     {
         return HttpGet(Url, postDataStr, 0);
     }

    public static string HttpGet(string Url, string postDataStr,int timeout)
    {
        string retString = null;
        try
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(Url + (postDataStr == "" ? "" : "?") + postDataStr);
            request.Method = "GET";
            request.ContentType = "text/html;charset=UTF-8";
            if (timeout>0)
              request.Timeout = timeout;
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
          
            Stream myResponseStream = response.GetResponseStream();
            StreamReader myStreamReader = new StreamReader(myResponseStream, Encoding.UTF8);
            retString = myStreamReader.ReadToEnd();
            myStreamReader.Close();
            myResponseStream.Close();
        }
        catch (Exception e)
        {
            return "error";
        }

        return retString;
    }

        /// <summary>  
        /// 返回JSon数据  
        /// </summary>  
        /// <param name="JSONData">要处理的JSON数据</param>  
        /// <param name="Url">要提交的URL</param>  
        /// <returns>返回的JSON处理字符串</returns>  
        public static string GetResponseData(string JSONData, string Url)  
            {
                try
                {
                    string serviceAddress = Url;
                    HttpWebRequest request = (HttpWebRequest)WebRequest.Create(serviceAddress);

                    request.Method = "POST";
                    request.ContentType = "application/json";
                    string strContent = JSONData;
                    using (StreamWriter dataStream = new StreamWriter(request.GetRequestStream()))
                    {
                        dataStream.Write(strContent);
                        dataStream.Close();
                    }
                    HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                    string encoding = response.ContentEncoding;
                    if (encoding == null || encoding.Length < 1)
                    {
                        encoding = "UTF-8"; //默认编码  
                    }
                    StreamReader reader = new StreamReader(response.GetResponseStream(), Encoding.GetEncoding(encoding));
                    string strResult = reader.ReadToEnd();

                    return strResult;  
                }
                catch (SmtpException ex)
                {
                    return null;
                }
               
            }


        public static string GetResponseData(string JSONData, string Url, int timeout)
        {
            try
            {
                string serviceAddress = Url;
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(serviceAddress);

                request.Method = "POST";
                request.ContentType = "application/json";
                if (timeout > 0)
                    request.Timeout = timeout;
                string strContent = JSONData;
                using (StreamWriter dataStream = new StreamWriter(request.GetRequestStream()))
                {
                    dataStream.Write(strContent);
                    dataStream.Close();
                }
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                string encoding = response.ContentEncoding;
                if (encoding == null || encoding.Length < 1)
                {
                    encoding = "UTF-8"; //默认编码  
                }
                StreamReader reader = new StreamReader(response.GetResponseStream(), Encoding.GetEncoding(encoding));
                string strResult = reader.ReadToEnd();

                return strResult;
            }
            catch (SmtpException ex)
            {
                return null;
            }

        }


        public static bool SendSMTPEMail(string strSmtpServer, string strFrom, string strFromPass, string strto, string strSubject, string strBody)
        {
            System.Net.Mail.SmtpClient client = new SmtpClient(strSmtpServer);
            client.UseDefaultCredentials = false;
            client.EnableSsl = true;
            client.Credentials = new System.Net.NetworkCredential(strFrom, strFromPass);
            client.DeliveryMethod = SmtpDeliveryMethod.Network;

            System.Net.Mail.MailMessage message = new MailMessage(strFrom, strto, strSubject, strBody);
            message.BodyEncoding = System.Text.Encoding.UTF8;
            message.IsBodyHtml = true;

            try
            {
                client.Send(message); // 发送邮件
                
            }
            catch (SmtpException ex)
            {
                return false;
            }

            return true;
             // 发送邮件
        }



        public static string ConvertUtfUrlPram(string Pram)
        {
            Encoding encoding = Encoding.GetEncoding("utf-8");
            Pram = HttpUtility.UrlEncode(Pram, encoding);
            return Pram;
        }

        public static string ConvertEncoding(string param, string coding)
        {
            Encoding encoding = Encoding.GetEncoding(coding);
            return ConvertEncoding(param, encoding);
        }


        public static string ConvertEncoding(string param, Encoding encoding)
        {

            string info = HttpUtility.UrlEncode(param, encoding);
            return info;
        }


        public static string ConvertDecoding(string param, Encoding encoding)
        {

            string info = HttpUtility.UrlDecode(param, encoding);
            return info;
        }
     

        /// <summary>
        /// 判断对象是否为Int32类型的数字
        /// </summary>
        /// <param name="Expression"></param>
        /// <returns></returns>
        public static bool IsNumeric(object Expression)
        {
            return TypeParse.IsNumeric(Expression);
        }
        /// <summary>
        /// 从HTML中获取文本,保留br,p,img
        /// </summary>
        /// <param name="HTML"></param>
        /// <returns></returns>
        public static string GetTextFromHTML(string HTML)
        {
            System.Text.RegularExpressions.Regex regEx = new System.Text.RegularExpressions.Regex(@"</?(?!br|/?p|img)[^>]*>", System.Text.RegularExpressions.RegexOptions.IgnoreCase);

            return regEx.Replace(HTML, "");
        }

        public static bool IsDouble(object Expression)
        {
            return TypeParse.IsDouble(Expression);
        }

        /// <summary>
        /// string型转换为bool型
        /// </summary>
        /// <param name="strValue">要转换的字符串</param>
        /// <param name="defValue">缺省值</param>
        /// <returns>转换后的bool类型结果</returns>
        public static bool StrToBool(object Expression, bool defValue)
        {
            return TypeParse.StrToBool(Expression, defValue);
        }

        /// <summary>
        /// 将对象转换为Int32类型
        /// </summary>
        /// <param name="strValue">要转换的字符串</param>
        /// <param name="defValue">缺省值</param>
        /// <returns>转换后的int类型结果</returns>
        public static int StrToInt(object Expression, int defValue)
        {
            return TypeParse.StrToInt(Expression, defValue);
        }

        /// <summary>
        /// string型转换为float型
        /// </summary>
        /// <param name="strValue">要转换的字符串</param>
        /// <param name="defValue">缺省值</param>
        /// <returns>转换后的int类型结果</returns>
        public static float StrToFloat(object strValue, float defValue)
        {
            return TypeParse.StrToFloat(strValue, defValue);
        }

        /// <summary>
        /// 判断给定的字符串数组(strNumber)中的数据是不是都为数值型
        /// </summary>
        /// <param name="strNumber">要确认的字符串数组</param>
        /// <returns>是则返加true 不是则返回 false</returns>
        public static bool IsNumericArray(string[] strNumber)
        {
            return TypeParse.IsNumericArray(strNumber);
        }

        

        /// <summary>
        /// 随机字符串
        /// </summary>
        /// <param name="codeCount"></param>
        /// <returns></returns>
        public static string GenerateCheckCode(int codeCount)
        {
            int rep = 0;
            string str = string.Empty;
            long num2 = DateTime.Now.Ticks + rep;
            rep++;
            Random random = new Random(((int)(((ulong)num2) & 0xffffffffL)) | ((int)(num2 >> rep)));
            for (int i = 0; i < codeCount; i++)
            {
                char ch;
                int num = random.Next();
                if ((num % 2) == 0)
                {
                    ch = (char)(0x30 + ((ushort)(num % 10)));
                }
                else
                {
                    ch = (char)(0x41 + ((ushort)(num % 0x1a)));
                }
                str = str + ch.ToString();
            }
            return str;
        }


        /// <summary>
        /// 随机抽选
        /// </summary>
        /// <param name="num"></param>
        /// <returns></returns>
        public static bool RandNumber(int num)
        {
            Random random = new Random();
            int value = 100 - num;
            if (random.Next(100) > value)
            {
                return false;//被选中
            }
            else
            {
                return true;//不被选中 
            }  
        }

        public static bool RandNumberThread(int num)
        {
            System.Threading.Thread.Sleep(1);
            Random random = new Random();
            int value = 100 - num;
            if (random.Next(100) > value)
            {
                return false;//被选中
            }
            else
            {
                return true;//不被选中 
            }
        }

        public static int RandNumberx(int num)
        {
            Random random = new Random();
            int value = 100 - num;
            if (random.Next(100) > value)
                return 2;//被选中
            else
                return 1;//不被选中
        }

        public static string getIp()
        {
            if (System.Web.HttpContext.Current.Request.ServerVariables["HTTP_VIA"] != null)
                return System.Web.HttpContext.Current.Request.ServerVariables["HTTP_X_FORWARDED_FOR"].Split(new char[] { ',' })[0];
            else
                return System.Web.HttpContext.Current.Request.ServerVariables["REMOTE_ADDR"];
        }

        /// <summary>
        /// 拼接sql
        /// </summary>
        /// <returns></returns>
        public static string StringAppend(string strNew, string Parmid, string inStr)
        {
            string result = string.Empty;
            if (!string.IsNullOrEmpty(Parmid) && Parmid != "")
            {

                if (!string.IsNullOrEmpty(strNew))
                    result += " " + inStr + " " + strNew + " ";
                else
                    result += " " + strNew + " ";

            }
            return result;
        }

        public static string StringAppend(string sqlstr, string strNew, string Parmid, string inStr)
        {
            string result = string.Empty;
            if (!string.IsNullOrEmpty(Parmid) && Parmid != "")
            {

                if (!string.IsNullOrEmpty(sqlstr))
                    result += " " + inStr + " " + strNew + " ";
                else
                    result += " " + strNew + " ";

            }
            return result;
        }

        public static string StringAppendx(string sqlstr, string strNew, string Parmid, string inStr)
        {
            string result = string.Empty;
            if (!string.IsNullOrEmpty(Parmid) && Parmid != "")
            {

                if (!string.IsNullOrEmpty(sqlstr))
                    result += " " + inStr + " " + strNew + " ";
                else
                    result += " " + strNew + " ";

            }
            return sqlstr + result;
        }

        /// <summary>
        /// 获取数据分页数据
        /// </summary>
        /// <param name="PageCount"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        public static int PageInfo(int PageCount, int PageSize)
        {
            if (PageCount % PageSize == 0)
                return PageCount / PageSize;
            else
                return ((PageCount / PageSize) + 1);
        }


        /// <summary>
        /// 设置格式化
        /// </summary>
        /// <param name="format"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static string setFormatValue(string format, object[] param)
        {
            int j = 0;
            string value = "";
            object[] pavalue = new object[param.Length];
            while (j < param.Length)
            {
                pavalue[j] = param[j].ToString();
                ++j;
            }
            value += string.Format(format, pavalue);

            return value;
        }

      
       public static int setMtable(string area, string stime, string etime, int productid)
       {
               int m = 0;
           
               if (null != mailTab[productid])
               {
                   JObject info=JObject.Parse(mailTab[productid].ToString());
                   
                   if (info["area"].ToString() == area)
                   {
                       if(info["stime"].ToString() == stime && info["etime"].ToString()==etime)
                          m = 1;
                       else
                       {
                          mailTab.Remove(productid);
                          string newinfo="{\"area\":\""+area+"\",\"stime\":\""+stime+"\",\"etime\":\""+etime+"\"}";
                          mailTab.Add(productid, newinfo);
                       }
                   }
                   else
                   {
                       mailTab.Remove(productid);
                       string newinfo="{\"area\":\""+area+"\",\"stime\":\""+stime+"\",\"etime\":\""+etime+"\"}";
                       mailTab.Add(productid, newinfo);
                       m = 0;
                   }
               }
               else
               {
                   string newinfo="{\"area\":\""+area+"\",\"stime\":\""+stime+"\",\"etime\":\""+etime+"\"}";
                   mailTab.Add(productid, newinfo);
                   m = 0;
               }
             return m;
           }


       public static int setMtable(string area, string stime, string etime, int productid, int conduitid)
       {
           int m = 0;

           if (null != mailTab[conduitid])
           {
               string cnd = mailTab[conduitid].ToString();
               JArray info = JArray.Parse(cnd);
               if (productid > 0)
               {
                   for (int i = 0; i < info.Count; i++)
                   {
                       if (Convert.ToInt32(info[i]["productid"]) == productid && info[i]["area"].ToString() == area)
                       {
                           m = 1;
                           break;
                       }
                   }
                   if (m == 0)
                   {
                       mailTab.Remove(conduitid);
                       string newinfo = "{\"area\":\"" + area + "\",\"productid\":" + productid + ",\"stime\":\"" + stime + "\",\"etime\":\"" + etime + "\"}";
                       info.Add(newinfo);
                       mailTab.Add(productid, info);
                   }
               }
               else
               {
                   if (cnd.IndexOf(area) > -1)
                       m = 1;
                   else
                   {
                       mailTab.Remove(conduitid);
                       string newinfo = "{\"area\":\"" + area + "\",\"productid\":" + productid + ",\"stime\":\"" + stime + "\",\"etime\":\"" + etime + "\"}";
                       info.Add(newinfo);
                       mailTab.Add(productid, info);
                   }
               }
           }
           else
           {
               string newinfo = "[{\"area\":\"" + area + "\",\"productid\":" + productid + ",\"stime\":\"" + stime + "\",\"etime\":\"" + etime + "\"}]";
               mailTab.Add(productid, newinfo);
           }
           return m;
       }

       public static int setMtable(JObject jsonObj)
       {

           string key = jsonObj["key"].ToString() + jsonObj["area"].ToString();

           if (null != areaMailTab[key])
           {
               string cnd = areaMailTab[key].ToString();
               JObject info = JObject.Parse(cnd);
               if (info["datatime"].ToString() == jsonObj["datatime"].ToString())
                   return 1;

               areaMailTab[key] = jsonObj.ToString();
           }
           else
           {
               string newkey = jsonObj["key"].ToString() + jsonObj["area"].ToString();
               areaMailTab.Add(newkey, jsonObj.ToString());
           }
           return 0;
       }




       public static int setAreaMtable(JObject jsonObj)
       {

           string key = jsonObj["key"].ToString() + jsonObj["area"].ToString();

           if (null != areaMailTab[key])
           {
               string cnd = areaMailTab[key].ToString();
               JObject info = JObject.Parse(cnd);
               if (info["datatime"].ToString() == jsonObj["datatime"].ToString())
                   return 1;

               areaMailTab[key] = jsonObj.ToString();
           }
           else
           {
               string newkey = jsonObj["key"].ToString() + jsonObj["area"].ToString();
               areaMailTab.Add(newkey, jsonObj.ToString());
           }
           return 0;
       }



       public static int setFeeMtable(JObject jsonObj)
       {

           string key = jsonObj["key"].ToString();

           if (null != feeMailTab[key])
           {
               string cnd = feeMailTab[key].ToString();
               JObject info = JObject.Parse(cnd);

               if (info["datatime"].ToString() == jsonObj["datatime"].ToString())
                   return 1;

               feeMailTab[key] = jsonObj.ToString();
           }
           else
               feeMailTab.Add(key, jsonObj.ToString());

           return 0;
       }



       public static int setWordMtable(int conduitid, int productid, string jsonObj, string resutl, string datatime, string area, ref string getkey, ref string message)
       {
           
               if (string.IsNullOrEmpty(jsonObj))
                  return 2;
           
               JArray jsondata = JArray.Parse(jsonObj);
               
               foreach (JObject item in jsondata)
               {
                   
                   string key=string.Format("{0}-{1}-{2}", conduitid,productid,item["flag"].ToString());
                  
                   if (null != wordMailTab[key])
                   {
                       JObject jsonda = JObject.Parse(wordMailTab[key].ToString());


                       if (null != item["outtime"])
                       {
                           int Minut = Convert.ToInt32(item["outtime"]);
                           string outtime = DateTime.Now.AddMinutes(-Minut).ToString("yyyy-MM-dd");
                           if (outtime != datatime)
                               break;
                       }

                       if (jsonda["datatime"].ToString() == datatime)
                       {

                           if (null != item["area"])
                           {
                               string areaItem = jsonda["area"].ToString();

                               if (areaItem.IndexOf(area) > -1)
                               {
                                   message = area + item["message"].ToString();
                                   return 1;
                               }

                               if (!string.IsNullOrEmpty(item["result"].ToString()))   
                                   getkey = item["result"].ToString();

                               JArray a = (JArray)jsonda["area"];
                               a.Add(area);
                               message = area+item["message"].ToString();
                               wordMailTab[key] = jsonda.ToString();

                               return 0;
                               
                           }
                           message =item["message"].ToString();
                           return 1;
                       }

                       if (!string.IsNullOrEmpty(item["result"].ToString()))
                           getkey = item["result"].ToString();

                       if (null != item["area"])
                       {
                           string areaItem = jsonda["area"].ToString();
                           JArray a = (JArray)jsonda["area"];
                           a.RemoveAll();
                           a.Add(area);
                           message = area+item["message"].ToString();
                       }
                       else
                           message = item["message"].ToString();

                       jsonda["datatime"] = datatime;
                       wordMailTab[key] = jsonda.ToString();   
                       return 0;
                       
                   }
                   else
                   {
                       if (resutl.IndexOf(item["word"].ToString()) == -1)
                           continue;

                       if (!string.IsNullOrEmpty(item["result"].ToString()))
                           getkey = item["result"].ToString();

                       if (null != item["area"])
                       {
                           JArray a = (JArray)item["area"];
                           a.Add(area);
                           message = area + item["message"].ToString();
                       }
                       else
                         message = item["message"].ToString();
                       item["datatime"] = datatime;
                       wordMailTab.Add(key,item.ToString());
                       return 0;
                   }
               }

           return 2;
       }

       

       public static string GetTimeStamp(System.DateTime time)
       {
           long ts = ConvertDateTimeToInt(time);
           return ts.ToString();
       }
       /// <summary>  
       /// 将c# DateTime时间格式转换为Unix时间戳格式  
       /// </summary>  
       /// <param name="time">时间</param>  
       /// <returns>long</returns>  
       public static long ConvertDateTimeToLongInt(System.DateTime time)
       {
           System.DateTime startTime = TimeZone.CurrentTimeZone.ToLocalTime(new System.DateTime(1970, 1, 1, 0, 0, 0, 0));
           long t = (time.Ticks - startTime.Ticks) / 10000;   //除10000调整为13位      
           return t;
       }



       public static long ConvertDateTimeToInt(System.DateTime time)
       {
           long t = (time.ToUniversalTime().Ticks - 621355968000000000) / 10000000;    
           return t;
       }

       /// <summary>        
       /// 时间戳转为C#格式时间        
       /// </summary>        
       /// <param name=”timeStamp”></param>        
       /// <returns></returns>        
       public static DateTime ConvertStringToDateTime(string timeStamp)
       {
           DateTime dtStart = TimeZone.CurrentTimeZone.ToLocalTime(new DateTime(1970, 1, 1));
           long lTime = long.Parse(timeStamp + "0000");
           TimeSpan toNow = new TimeSpan(lTime);
           return dtStart.Add(toNow);
       }


       
        public static string UnBase64String(string value)
        {
            if (value == null || value == "")
            {
                return "";
            }
            byte[] bytes = Convert.FromBase64String(value);
            return Encoding.UTF8.GetString(bytes);
        }


        public static string ToBase64String(string value)
        {
            if (value == null || value == "")
            {
                return "";
            }
            byte[] bytes = Encoding.UTF8.GetBytes(value);
            return Convert.ToBase64String(bytes);
        }


        /// <summary> /// 字符串转Unicode码 /// 
        /// </summary> /// <returns>The to unicode.</returns> 
        /// <param name="value">Value.</param> 
        public static string StringToUnicode(string value) 
        { 
            byte[] bytes = Encoding.Unicode.GetBytes (value); 
            StringBuilder stringBuilder = new StringBuilder (); 
            for (int i = 0; i < bytes.Length; i += 2) 
            { // 取两个字符，每个字符都是右对齐。 
                stringBuilder.AppendFormat ("u{0}{1}", bytes [i + 1].ToString ("x").PadLeft (2, '0'), bytes [i].ToString ("x").PadLeft (2, '0')); 
            } 
           return stringBuilder.ToString (); 
        }


        /// <summary> /// Unicode转字符串 /// </summary> 
        /// <returns>The to string.</returns> 
        /// <param name="unicode">Unicode.</param> 
        public static string UnicodeToString(string unicode) 
        {
          
            string outStr = "";  
            Regex reg = new Regex(@"(?i)//u([0-9a-f]{4})");
            outStr = reg.Replace(unicode, delegate(Match m1)  
            {  
                return ((char)Convert.ToInt32(m1.Groups[1].Value, 16)).ToString();  
            });  
            return outStr;  
            
        }


       }//end class

}//end namespace
