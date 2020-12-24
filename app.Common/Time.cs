using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;

namespace app.Common
{
   public class Time
    {
        /// <summary>
        /// 获取当前的日期是本周的第几个星期
        /// </summary>
        /// <returns></returns>
        public string GetDateWeek()
        {
            string currentTime = DateTime.Now.ToShortDateString();
            DateTime searchday = Convert.ToDateTime(currentTime);
            return searchday.DayOfWeek.ToString();
        }


        public int GetDateWeek(DateTime searchday)
        {
            int weeks = 1;
            string week = searchday.DayOfWeek.ToString();

            switch (week)
            {
                case "Monday":
                    weeks = 1;
                    break;
                case "Tuesday":
                    weeks = 2;
                    break;
                case "Wednesday":
                    weeks = 3;
                    break;
                case "Thursday":
                    weeks = 4;
                    break;
                case "Friday":
                    weeks = 5;
                    break;
                case "Saturday":
                    weeks = 6;
                    break;
                case "Sunday":
                    weeks = 7;
                    break;
            }
            return weeks;

        }

        /// <summary>
        /// 得到本周第一天(以星期天为第一天)
        /// </summary>
        /// <param name="datetime"></param>
        /// <returns></returns>
        public DateTime GetWeekFirstDaySun(DateTime datetime)
        {
            //星期天为第一天
            int weeknow = Convert.ToInt32(datetime.DayOfWeek);
            int daydiff = (-1) * weeknow;

            //本周第一天
            string FirstDay = datetime.AddDays(daydiff).ToString("yyyy-MM-dd");
            return Convert.ToDateTime(FirstDay);
        }

        /// <summary>
        /// 得到本周第一天(以星期一为第一天)
        /// </summary>
        /// <param name="datetime"></param>
        /// <returns></returns>
        public DateTime GetWeekFirstDayMon(DateTime datetime)
        {
            //星期一为第一天
            int weeknow = Convert.ToInt32(datetime.DayOfWeek);

            //因为是以星期一为第一天，所以要判断weeknow等于0时，要向前推6天。
            weeknow = (weeknow == 0 ? (7 - 1) : (weeknow - 1));
            int daydiff = (-1) * weeknow;

            //本周第一天
            string FirstDay = datetime.AddDays(daydiff).ToString("yyyy-MM-dd");
            return Convert.ToDateTime(FirstDay);
        }

        /// <summary>
        /// 得到本周最后一天(以星期六为最后一天)
        /// </summary>
        /// <param name="datetime"></param>
        /// <returns></returns>
        public DateTime GetWeekLastDaySat(DateTime datetime)
        {
            //星期六为最后一天
            int weeknow = Convert.ToInt32(datetime.DayOfWeek);
            int daydiff = (7 - weeknow) - 1;

            //本周最后一天
            string LastDay = datetime.AddDays(daydiff).ToString("yyyy-MM-dd");
            return Convert.ToDateTime(LastDay);
        }

        /// <summary>
        /// 得到本周最后一天(以星期天为最后一天)
        /// </summary>
        /// <param name="datetime"></param>
        /// <returns></returns>
        public DateTime GetWeekLastDaySun(DateTime datetime)
        {
            //星期天为最后一天
            int weeknow = Convert.ToInt32(datetime.DayOfWeek);
            weeknow = (weeknow == 0 ? 7 : weeknow);
            int daydiff = (7 - weeknow);

            //本周最后一天
            string LastDay = datetime.AddDays(daydiff).ToString("yyyy-MM-dd");
            return Convert.ToDateTime(LastDay);
        }

        /// 取得某月的第一天
        /// </summary>
        /// <param name="datetime">要取得月份第一天的日期</param>
        /// <returns></returns>
        public DateTime FirstDayOfMonth(DateTime datetime)
        {
            return datetime.AddDays(1 - datetime.Day);
        }

        //// <summary>
        /// 取得某月的最后一天
        /// </summary>
        /// <param name="datetime">要取得月份最后一天的日期</param>
        /// <returns></returns>
        public DateTime LastDayOfMonth(DateTime datetime)
        {
            return datetime.AddDays(1 - datetime.Day).AddMonths(1).AddDays(-1);
        }

        //// <summary>
        /// 取得上个月第一天
        /// </summary>
        /// <param name="datetime">要取得上个月第一天的日期</param>
        /// <returns></returns>
        public DateTime FirstDayOfPreviousMonth(DateTime datetime)
        {
            return datetime.AddDays(1 - datetime.Day).AddMonths(-1);
        }

        //// <summary>
        /// 取得上个月的最后一天
        /// </summary>
        /// <param name="datetime">要取得上个月最后一天的日期</param>
        /// <returns></returns>
        public DateTime LastDayOfPrdviousMonth(DateTime datetime)
        {
            return datetime.AddDays(1 - datetime.Day).AddDays(-1);
        }


        /// <summary>
        /// 获取当前的日期是本周的第几个星期
        /// </summary>
        /// <param name="Language">Chinese或English</param>
        /// <returns></returns>
        public int GetDateWeek(string Language)
        {
            int weeks = 1;
            string week = GetDateWeek();

            switch (week)
            {
                case "Monday":
                    weeks = 1;
                    break;
                case "Tuesday":
                    weeks = 2;
                    break;
                case "Wednesday":
                    weeks = 3;
                    break;
                case "Thursday":
                    weeks = 4;
                    break;
                case "Friday":
                    weeks = 5;
                    break;
                case "Saturday":
                    weeks = 6;
                    break;
                case "Sunday":
                    weeks = 0;
                    break;
            }
            return weeks;

        }


        /// <summary>
        /// 获取本周时间段
        /// </summary>
        /// <returns></returns>
        public string GetWeek()
        {

            string weeks = null;
            string week = GetDateWeek();
            //本周最后一天原本为：DateTime.Now.ToShortDateString(),为适应查询语句 故改为 DateTime.Now.AddDays(1).ToShortDateString()
            switch (week)
            {
                case "Monday":
                    weeks = "'" + DateTime.Now.ToShortDateString() + "' and '" + DateTime.Now.AddDays(1).ToShortDateString() + "'";
                    break;
                case "Tuesday":
                    weeks = "'" + DateTime.Now.AddDays(-1).ToShortDateString() + "' and '" + DateTime.Now.AddDays(1).ToShortDateString() + "'";
                    break;
                case "Wednesday":
                    weeks = "'" + DateTime.Now.AddDays(-2).ToShortDateString() + "' and '" + DateTime.Now.AddDays(1).ToShortDateString() + "'";
                    break;
                case "Thursday":
                    weeks = "'" + DateTime.Now.AddDays(-3).ToShortDateString() + "' and '" + DateTime.Now.AddDays(1).ToShortDateString() + "'";
                    break;
                case "Friday":
                    weeks = "'" + DateTime.Now.AddDays(-4).ToShortDateString() + "' and '" + DateTime.Now.AddDays(1).ToShortDateString() + "'";
                    break;
                case "Saturday":
                    weeks = "'" + DateTime.Now.AddDays(-5).ToShortDateString() + "' and '" + DateTime.Now.AddDays(1).ToShortDateString() + "'";
                    break;
                case "Sunday":
                    weeks = "'" + DateTime.Now.AddDays(-6).ToShortDateString() + "' and '" + DateTime.Now.AddDays(1).ToShortDateString() + "'";
                    break;
            }

            return weeks;
        }


        /// <summary>
        /// 获取上周时间段
        /// </summary>
        /// <returns></returns>
        public string GetPreviWeek()
        {
            string weeks = null;
            string week = GetDateWeek();
            switch (week)
            {
                case "Monday":
                    weeks = "'" + DateTime.Now.AddDays(-7).ToShortDateString() + "' and '" + DateTime.Now.AddDays(-1).ToShortDateString() + "'";
                    break;
                case "Tuesday":
                    weeks = "'" + DateTime.Now.AddDays(-8).ToShortDateString() + "' and '" + DateTime.Now.AddDays(-2).ToShortDateString() + "'";
                    break;
                case "Wednesday":
                    weeks = "'" + DateTime.Now.AddDays(-9).ToShortDateString() + "' and '" + DateTime.Now.AddDays(-3).ToShortDateString() + "'";
                    break;
                case "Thursday":
                    weeks = "'" + DateTime.Now.AddDays(-10).ToShortDateString() + "' and '" + DateTime.Now.AddDays(-4).ToShortDateString() + "'";
                    break;
                case "Friday":
                    weeks = "'" + DateTime.Now.AddDays(-11).ToShortDateString() + "' and '" + DateTime.Now.AddDays(-5).ToShortDateString() + "'";
                    break;
                case "Saturday":
                    weeks = "'" + DateTime.Now.AddDays(-12).ToShortDateString() + "' and '" + DateTime.Now.AddDays(-6).ToShortDateString() + "'";
                    break;
                case "Sunday":
                    weeks = "'" + DateTime.Now.AddDays(-13).ToShortDateString() + "' and '" + DateTime.Now.AddDays(-7).ToShortDateString() + "'";
                    break;
            }

            return weeks;
        }



        /// <summary>
        /// 获取本月时间段
        /// </summary>
        /// <returns></returns>
        public string GetMonth()
        {
            DateTime monthDays = new DateTime(DateTime.Now.Year, (DateTime.Now.Month), 1).AddMonths(1).AddDays(-1);
            string Months = "'" + DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-01' and '" + monthDays.ToString("yyyy-MM-dd") + "'";

            return Months;
        }

        public ArrayList GetMonth(DateTime dat)
        {
            int Month = dat.Month;

            DateTime monthDays = new DateTime(DateTime.Now.Year, (Month), 1).AddMonths(1).AddDays(-1);
            ArrayList Months = new ArrayList();

            int date = monthDays.Day;
            for (int i = 1; i <= date; i++)
                Months.Add(i);

            return Months;
        }

        /// <summary>
        /// 获取上月时间段
        /// </summary>
        /// <returns></returns>
        public string GetPreviMonth()
        {

            System.DateTime currentTime = new System.DateTime();
            currentTime = System.DateTime.Now;
            int Year = currentTime.Year;
            int Month = currentTime.Month;

                /*获取某个月的最后一天是几号*/
            DateTime monthDays = new DateTime(DateTime.Now.Year, (Month - 1), 1).AddMonths(1).AddDays(-1);

            return "'" + Convert.ToString(Year) + "-" + (Month - 1) + "-1' and '" + monthDays.ToShortDateString() + "'";
            
        }

        /// <summary>
        /// 获取当月最后一天日期
        /// </summary>
        /// <returns></returns>
        public string GetMonthLastDate()
        {
            DateTime currentTime = System.DateTime.Now;
            int Month = currentTime.Month;
            DateTime monthDays = new DateTime(DateTime.Now.Year, Month, 1).AddMonths(1).AddDays(-1);

            return monthDays.ToString("yyyy-MM-dd");
        }


        /// <summary>
        /// 获取今年到当前时间段
        /// </summary>
        /// <returns></returns>
        public string GetYear()
        {
            return "'" + Convert.ToString(DateTime.Now.Year) + "-01-01' and '" + DateTime.Now.AddMonths(0).AddDays(0).ToShortDateString() + "'";
        }

        /// <summary>
        /// 获取今天时间段
        /// </summary>
        /// <returns></returns>
        public string GetToday()
        {
            return "'" + DateTime.Now.ToString("yyyy-MM-dd") + "' and '" + DateTime.Now.AddDays(1).ToString("yyyy-MM-dd") + "'";
        }


        /// <summary>
        /// 获取当前小时
        /// </summary>
        /// <returns></returns>
        public int GetNowHour()
        {
            return Convert.ToInt32(DateTime.Now.Hour);
        }

        /// <summary>
        /// 获取当前剩余的小时
        /// </summary>
        /// <returns></returns>
        public int GetSurplusHour()
        {
            return 24 - Convert.ToInt32(DateTime.Now.Hour);
        }

        /// <summary>
        /// 获取本月的当前是几号
        /// </summary>
        /// <returns></returns>
        public int GetNowDay()
        {
            return Convert.ToInt32(DateTime.Now.Day);
        }

        /// <summary>
        /// 获取24小时
        /// </summary>
        /// <returns></returns>
        public ArrayList GetHour()
        {
            ArrayList dhour = new ArrayList();
            string itmes = string.Empty;
            for (int i = 0; i < 24; i++)
            {
                if (i < 10)
                {
                    itmes = "0" + i;
                    dhour.Add(itmes);
                }
                else
                    dhour.Add(i);
            }
            return dhour;
        }


        /// <summary>
        /// 获取60分
        /// </summary>
        /// <returns></returns>
        public ArrayList GetSecs()
        {
            ArrayList secs = new ArrayList();
            string itmes = string.Empty;
            for (int i = 0; i < 12; i++)
            {
                if (i < 2)
                {
                    itmes = "0" + (i * 5);
                    secs.Add(itmes);
                }
                else
                    secs.Add(i * 5);
            }
            return secs;
        }

        /// <summary>
        /// 获取查询时间段
        /// </summary>
        /// <param name="setid"></param>
        /// <returns></returns>
        public string GetDate(int setid)
        {
            string mydate = null;

            switch (setid)
            {
                case 0:
                    mydate = this.GetMonth();
                    break;
                case 1:
                    mydate = this.GetToday();
                    break;
                case 2:
                    mydate = this.GetWeek();
                    break;
                case 3:
                    mydate = this.GetPreviWeek();
                    break;
                case 4:
                    mydate = this.GetPreviMonth();
                    break;
                case 5:
                    mydate = this.GetYear();
                    break;
            }

            return mydate;
        }


    }
}
