using System;
using System.Collections.Generic;
using System.Text;

namespace DataAggregator
{
    public static class StringHelpers
    {
		public static void SendFeedback(string message, string identifier = "")
		{
			string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
			System.Console.WriteLine(dt_string + message + identifier);
		}

		public static void SendHeader(string message)
		{
			string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
			System.Console.WriteLine("");
			System.Console.WriteLine(dt_string + "**** " + message + " ****");
		}

		public static void SendError(string message)
		{
			string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
			System.Console.WriteLine("");
			System.Console.WriteLine("+++++++++++++++++++++++++++++++++++++++");
			System.Console.WriteLine(dt_string + "***ERROR*** " + message);
			System.Console.WriteLine("+++++++++++++++++++++++++++++++++++++++");
			System.Console.WriteLine("");
		}
	}
}
