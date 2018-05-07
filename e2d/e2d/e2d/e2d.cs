using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.ServiceProcess;
using System.Text;
using Microsoft.Win32;
using MySql.Data.MySqlClient;
using Oracle.DataAccess.Client;

namespace e2d
{
    public partial class e2d : ServiceBase
    {
        public e2d()
        {
            InitializeComponent();
        }

        string _filePath;
        bool _writeLog = true;
        private System.Timers.Timer _timer = null;
        private bool _isBusy = false;//正在同步
        private int _hourIndex = 0;//要执行的时间序号
        private int[] _hours;//要执行的小时
        private int[] _minute;//要执行的小时对应的分钟

        private string _oracleConnStr = "";
        private string _myConnStr = "";

        //private List<int> _updateDeptID, _updateEmpID, _updateDeptPositionID, _insertEmpID;

        protected override void OnStart(string[] args)
        {
            _filePath = GetWindowsServiceInstallPath("e2d");
            WriteLog("Start");
            WriteLog("Init Data");
            InitData();
            WriteLog("End Init");
        }

        protected override void OnStop()
        {
            _timer.Enabled = false;
            _timer.Dispose();

            WriteLog("End");
        }


        protected void InitData()
        {
            try
            {
                //_updateDeptID = new List<int>();
                //_updateEmpID = new List<int>();
                //_insertEmpID = new List<int>();

                string filePath = _filePath + @"\e2d.txt";

                FileStream fs = new FileStream(filePath, FileMode.Open);//打开数据文件
                StreamReader sr = new StreamReader(fs);
                bool immediately = (Convert.ToInt32(sr.ReadLine()) == 1) ? true : false;
                _writeLog = (Convert.ToInt32(sr.ReadLine()) == 1) ? true : false;

                //定时启动的时间设置 begin
                string hour = sr.ReadLine();
                string[] hours = hour.Split(',');
                _hours = new int[hours.Length];
                _minute = new int[hours.Length];
                
                for (int i = 0; i < hours.Length; i++)
                {
                    string[] tempTime = hours[i].Split(':');
                    _hours[i] = Convert.ToInt32(tempTime[0]);

                    if (tempTime.Length == 1)
                        _minute[i] = 0;
                    else
                        _minute[i] = Convert.ToInt32(tempTime[1]);

                }
                _hourIndex = 0;
                //定时启动的时间设置 end

                _oracleConnStr = sr.ReadLine();
                _myConnStr = sr.ReadLine();

                fs.Close();
                sr.Close();

                WriteLog("immediately:" + immediately.ToString() + ";hour:" + hour + ";fromDB:" + _oracleConnStr + ";toDB:" + _myConnStr);
               
                if (immediately)
                    SynchronousData();

                InitTimer();
            }
            catch (Exception ex)
            {
                WriteLog(ex.ToString());
            }
        }

        protected void InitTimer()
        {
            WriteLog("Init Timer");
            _timer = new System.Timers.Timer(1000 * 60);//一分钟判断一次
            _timer.Enabled = true;
            _timer.Elapsed += this.TimeOut;
            _timer.Start();
        }


        protected void WriteLog(string text)
        {
            if (!_writeLog)
                return;

            string filePath = _filePath + @"\e2d_log.txt";
            FileStream fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.Write);
            StreamWriter streamWriter = new StreamWriter(fs);
            streamWriter.BaseStream.Seek(0, SeekOrigin.End);
            streamWriter.WriteLine(DateTime.Now.ToString() + " " + text);
            streamWriter.Flush();
            streamWriter.Close();
            fs.Close();
        }

        private void TimeOut(object sender, EventArgs e)
        {
            if (DateTime.Now.Hour == _hours[_hourIndex] && DateTime.Now.Minute == _minute[_hourIndex])
            {
                SynchronousData();
            }
        }

        public static string GetWindowsServiceInstallPath(string ServiceName)
        {
            string key = @"SYSTEM\CurrentControlSet\Services\" + ServiceName;
            string path = Registry.LocalMachine.OpenSubKey(key).GetValue("ImagePath").ToString();
            path = path.Replace("\"", string.Empty);//替换掉双引号
            FileInfo fi = new FileInfo(path);
            return fi.Directory.ToString();
        }

        protected void SynchronousData()
        {
            if (_isBusy)
                return;

            _isBusy = true;
            WriteLog("synchronous start");

            try
            {
                //_updateDeptID.Clear();
                //_updateEmpID.Clear();
                //_insertEmpID.Clear();

                using (MySqlConnection _myConn = new MySqlConnection(_myConnStr))
                {
                    MySqlCommand _myCmd = _myConn.CreateCommand();
                    _myCmd.CommandText = "select `sort_index`,`from_table`,`to_table`,`from_fields`,`to_fields`,`from_id`,`to_id`,`to_where`,`from_order` from sys_data_synchronous order by sort_index";
                    _myConn.Open();
                    MySqlDataReader _myReader = _myCmd.ExecuteReader();

                    if (_myReader.HasRows)
                    {
                        using (OracleConnection oracleConn = new OracleConnection(_oracleConnStr))
                        {
                            OracleCommand oracleCmd = oracleConn.CreateCommand();
                            OracleDataReader oracleReader;
                            oracleConn.Open();

                            MySqlConnection myConn = new MySqlConnection(_myConnStr);

                            while (_myReader.Read())
                            {
                                oracleCmd.CommandText = "select " + _myReader["from_fields"] + " from " + _myReader["from_table"] + (_myReader["from_order"].ToString().Length > 1 ? (" order by " + _myReader["from_order"].ToString()) : "");

                                //WriteLog("++++++++"+oracleCmd.CommandText);

                                oracleReader = oracleCmd.ExecuteReader();
                                if (oracleReader.HasRows)
                                {
                                    string fromField = _myReader["from_fields"].ToString().Replace(",'yyyy-mm-dd') ","");
                                    string[] fromFields = fromField.Split(',');

                                    for (int i = 0; i < fromFields.Length; i++)
                                    {
                                        fromFields[i] = fromFields[i].TrimStart(' ').TrimEnd(' ');
                                        if (fromFields[i].IndexOf(" ") > -1)
                                        { 
                                            string[] strTemp =fromFields[i].Split(' ');
                                            fromFields[i] = strTemp[strTemp.Length - 1];
                                        }
                                    }

                                    fromField = _myReader["to_fields"].ToString().Replace(",'%Y-%m-%d')", "");
                                    string[] toFields = fromField.Split(',');
                                    
                                    for (int i = 0; i < toFields.Length; i++)
                                    {
                                        toFields[i] = toFields[i].TrimStart(' ').TrimEnd(' ');
                                        if (toFields[i].IndexOf(" ") > -1)
                                        {
                                            string[] strTemp = toFields[i].Split(' ');
                                            toFields[i] = strTemp[strTemp.Length - 1];
                                        }
                                    }

                                    List<string> updateSqls = new List<string>();
                                    StringBuilder sqlStr = new StringBuilder();

                                    MySqlDataAdapter mydp = new MySqlDataAdapter("select " + _myReader["to_fields"] + " from " + _myReader["to_table"]
                                        + (_myReader["to_where"].ToString().Length > 1 ? (" where " + _myReader["to_where"].ToString()) : ""), myConn);
                                    myConn.Open();
                                    DataTable dt = new DataTable();

                                    //WriteLog("------select " + _myReader["to_fields"] + " from " + _myReader["to_table"] + (_myReader["to_where"].ToString().Length > 1 ? (" where " + _myReader["to_where"].ToString()) : ""));

                                    //mydp.FillSchema(dt, SchemaType.Source);
                                    mydp.Fill(dt);
                                    dt.PrimaryKey = new DataColumn[] { dt.Columns[_myReader["to_id"].ToString()] };
                                    while (oracleReader.Read())
                                    {
                                        sqlStr.Clear();

                                        DataRow dRow = dt.Rows.Find(oracleReader[_myReader["from_id"].ToString()].ToString());//dt.Select(_myReader["to_id"].ToString() + "=" + oracleReader[_myReader["from_id"].ToString()].ToString()); //
                                        if (dRow != null)
                                        {
                                            for (int i = 0; i < fromFields.Length; i++)
                                            {
                                                if (dRow[toFields[i]].ToString() != oracleReader[fromFields[i]].ToString()
                                                    && toFields[i].ToString() != "create_time")
                                                {
                                                    //WriteLog(toFields[i].ToString() + ":" + dRow[toFields[i]].ToString() + "  " + fromFields[i].ToString() + ":" + oracleReader[fromFields[i]].ToString());

                                                    sqlStr.Append("update " + _myReader["to_table"] + " set ");

                                                    for (int f = 0; f < fromFields.Length; f++)
                                                    {
                                                        sqlStr.Append( toFields[f] + "='" + oracleReader[fromFields[f]] + "'");

                                                        if (f != fromFields.Length - 1)
                                                            sqlStr.Append(",");
                                                    }
                                                    sqlStr.Append(" where " + _myReader["to_id"] + "=" + oracleReader[_myReader["from_id"].ToString()].ToString()+";");
                                                    updateSqls.Add(sqlStr.ToString());
                                                    break;
                                                }
                                            }
                                        }
                                        else
                                        {
                                            sqlStr.Append("insert into " + _myReader["to_table"] + "(");
                                            for (int f = 0; f < fromFields.Length; f++)
                                            {
                                                sqlStr.Append(toFields[f].ToString());

                                                if (f != fromFields.Length - 1)
                                                    sqlStr.Append(",");
                                            }
                                            sqlStr.Append(") values (");

                                            for (int f = 0; f < fromFields.Length; f++)
                                            {
                                                sqlStr.Append( "'" + oracleReader[fromFields[f]] + "'");
                                                if (f != fromFields.Length - 1)
                                                    sqlStr.Append( ",");
                                            }
                                            sqlStr.Append(");");

                                            updateSqls.Add(sqlStr.ToString());
                                        }
                                        sqlStr.Clear();
                                        
                                        switch (_myReader["to_table"].ToString())
                                        {
                                            case "hr_department":
                                                sqlStr.Append(AddDeptUpdate(dRow, oracleReader));
                                                break;
                                            //case "hr_employee":
                                            //    sqlStr.Append(EmpUpdate(dRow[0], oracleReader));
                                            //    break;
                                            //case "hr_dept_position":
                                            //    sqlStr.Append(DeptPositionUpdate(dRow[0], oracleReader));
                                            //    break;
                                            case "sys_user":
                                                sqlStr.Append(UserUpdate(dRow, oracleReader));
                                                break;
                                        }

                                        if (sqlStr.Length > 1)
                                            updateSqls.Add(sqlStr.ToString());
                                       
                                    }
                                    myConn.Close();
                                    myConn.Dispose();

                                    UpdateData(_myReader["to_table"].ToString(), updateSqls);
                                }
                                oracleReader.Close();
                                oracleReader.Dispose();
                            }
                            oracleCmd.Cancel();
                            oracleCmd.Dispose();
                            oracleConn.Close();
                            oracleConn.Dispose();
                        }
                    }
                    else
                    {
                        WriteLog("no synchronous data");
                    }

                    _myReader.Close();
                    _myReader.Dispose();
                    _myCmd.Cancel();
                    _myCmd.Dispose();

                    _myConn.Close();
                    _myConn.Dispose();
                }
            }
            catch (Exception ex)
            {
                WriteLog(ex.ToString());
            }

            WriteLog("synchronous end");

            _hourIndex++;
            if (_hourIndex >= _hours.Length)
                _hourIndex = 0;

            _isBusy = false;
        }

        protected void UpdateData(string toTableName,List<string> updateSqls)
        {
            if (updateSqls.Count <= 0)
                return;

            StringBuilder sqlStr = new StringBuilder();
            
                WriteLog("update " + toTableName);
                WriteLog("total: " + updateSqls.Count);
                using (MySqlConnection conn = new MySqlConnection(_myConnStr))
                {
                    try
                    {
                        
                        MySqlCommand cmd = conn.CreateCommand(); 
                        conn.Open();
                        for (int i = 0; i < updateSqls.Count; i++)
                        {
                            sqlStr.Append(updateSqls[i]);
                            if ((i + 1) % 10 == 0)//5条5条地更新以免超时
                            {
                                cmd.CommandText = sqlStr.ToString();
                                cmd.ExecuteNonQuery();
                                //WriteLog(sqlStr.ToString());
                                sqlStr.Clear();
                            }
                        }

                        if (sqlStr.Length > 0)
                        {
                            cmd.CommandText = sqlStr.ToString();
                            cmd.ExecuteNonQuery();
                            //WriteLog(sqlStr.ToString());
                            sqlStr.Clear();
                        }
                        cmd.Cancel();
                        cmd.Dispose();
                    }
                    catch (Exception ex)
                    {
                        WriteLog(ex.ToString());
                        WriteLog(sqlStr.ToString());
                    }
                    finally
                    {
                        conn.Close();
                    }

                }
            
        }


        /// <summary>
        /// 部门信息更新
        /// </summary>
        /// <param name="dRow"></param>
        /// <param name="oracleReader"></param>
        /// <returns></returns>
        protected string AddDeptUpdate(DataRow dRow, OracleDataReader oracleReader)
        {
            bool bUpdate = false;
            StringBuilder updateStr = new StringBuilder();
            if (dRow != null)
            {
                if (dRow["dept_name"].ToString() != oracleReader["hr_dept_name"].ToString()
                    || dRow["dept_no"].ToString() != oracleReader["hr_dept_path"].ToString())
                {
                    bUpdate = true;

                    //if (dRow["dept_no"].ToString() != oracleReader["hr_dept_path"].ToString())
                    //    _updateDeptID.Add(Convert.ToInt32(oracleReader["hr_dept_id"].ToString()));
                }
            }
            else
            {
                bUpdate = true;
            }

            if (bUpdate == true)
            { 
                updateStr.Append("update `hr_department`, `hr_department` p set `hr_department`.dept_fullname = concat(p.dept_fullname,'>',hr_department.`dept_name`) ");
                updateStr.Append(" where p.`dept_no` = left(hr_department.`dept_no`,LENGTH(hr_department.`dept_no`)-2) and `hr_department`.dept_id =" + oracleReader["hr_dept_id"].ToString()+";");
            }

            return updateStr.ToString();

        }

        /// <summary>
        /// 用户信息更新
        /// </summary>
        /// <param name="dRow"></param>
        /// <param name="oracleReader"></param>
        /// <returns></returns>
        protected string UserUpdate(DataRow dRow, OracleDataReader oracleReader)
        {
            StringBuilder updateStr = new StringBuilder();
            if (dRow == null)
            {
                updateStr.Append("update sys_user,hr_employee e set sys_user.is_del=1,user_no='',");
                updateStr.Append("sys_user.e_name = e.`e_name`,sys_user.`tel`=e.`e_phone`,sys_user.`phone`=e.`e_mobile` ");
                updateStr.Append(" where sys_user.e_uid=e.e_id and sys_user.e_uid = " + oracleReader["e_user_id"].ToString()+";");
            }

            return updateStr.ToString();
        }
        
    }
}
