using System.ServiceProcess;

namespace e2d
{
    static class Program
    {
        /// <summary>
        /// 应用程序的主入口点。
        /// </summary>
        static void Main()
        {
            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[] 
			{ 
				new e2d() 
			};
            ServiceBase.Run(ServicesToRun);
        }
    }
}
