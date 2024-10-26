
using OneBrcUtilities;
using System.Globalization;

namespace OneBrcTests
{
    [TestClass]
    public class DoubleParseTest
    {
        public DoubleParseTest() 
        {
            CultureInfo cultureInfo = CultureInfo.CreateSpecificCulture("en-US");
            Thread.CurrentThread.CurrentCulture = cultureInfo;        
        }

        [TestMethod]
        public void FullRange_allPass()
        {            
            string[] inputValue = { "5", "-4", "74", "-99.9", "-11.5", "-7.4", "99.9", "11.5", "7.4", "0", "0.0", "-0", "-0.0", "-00.0"};
            double outputValue = 0;

            foreach (string strValue in inputValue) 
            { 
                byte[] buf = new byte[6];
                var valueStr = strValue.ToCharArray();

                for (int i = 0; i < valueStr.Length; i++) buf[i] = (byte)valueStr[i];

                outputValue = OneBrcUtility.ParseDouble(buf, 0, valueStr.Length);

                double reff = double.Parse(strValue);
                Assert.AreEqual(outputValue, reff, 0.01);
            }
        }
    }
}