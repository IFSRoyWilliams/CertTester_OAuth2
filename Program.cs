using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using Simple.OData.Client;
using System.Reflection;
using System.Data;
using System.Linq;
using Newtonsoft.Json;
using System.Globalization;
using System.Collections;
using NodaTime;
using System.Text;

namespace CertTester_OAuth2
{
    class Program
    {
        public static string clientId = "IFS_boomi";
        public static string clientSecret = "G5jxPLScPzaeM5lG1SB7";
        public static string tokenEndpoint = "https://borrdrilling-cfg.ifs.cloud/auth/realms/borrcfg1/protocol/openid-connect/token";
        public static string authEndpoint = "cfg.ifs.cloud/main/ifsapplications/projection/v1/ApplicationMessagesHandling.svc/ApplicationMessageSet?$filter=((contains(tolower(Queue),'data_sync')))&$orderby=ApplicationMessageId desc";
        public static string baseURI = "https://borrdrilling-cfg.ifs.cloud/main/ifsapplications/projection/v1/ApplicationMessagesHandling.svc";

        public static string TableName = "ApplicationMessageSet";
        public static string Filter = "((contains(tolower(Queue),'data_sync')))";
        public static string Filter2 = "{TimeStampField} gt datetime'{scanDateTimeStr}' and {TimeStampField} le datetime'{todayDateTimeStr}'";
        public static string OrderBy = "ApplicationMessageId desc";
        public static string TimeStampField = "CreatedDate";
        public static string NameField = "Queue";
        public static string Columns = "SeqNo";
        public static List<string> ListColumns = new List<string>() { "Queue", "CreatedDate", "SeqNo", "State" };
        public static bool WideMode = true;
        public static string Delimiter = ".";

        public static string hcat = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJDTHp1U2VuTXdYVWVNLXdEMXlQdFJEc1VkYWlJSE1RZ0JkX2diYnB3QXdFIn0.eyJleHAiOjE3NDc3MTYwOTUsImlhdCI6MTc0NzcxMjQ5NSwianRpIjoiMDIzZmE1YTctYjUxYi00NDczLWE1MjgtYzYwYjc3MzM2YzlhIiwiaXNzIjoiaHR0cHM6Ly9ib3JyZHJpbGxpbmctY2ZnLmlmcy5jbG91ZC9hdXRoL3JlYWxtcy9ib3JyY2ZnMSIsImF1ZCI6WyJib3JyY2ZnMSIsImFjY291bnQiXSwic3ViIjoiZTcxNTE4M2ItYzM3My00NjE4LTlkMzEtN2Y5ODQ3MDQyZTNjIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiSUZTX2Jvb21pIiwic2Vzc2lvbl9zdGF0ZSI6ImM0NDc4ZmNmLWJhMDAtNGYyOS04Y2IzLWQ1NjBhOWNkYjU2ZSIsImFsbG93ZWQtb3JpZ2lucyI6WyJodHRwczovL2JvcnJkcmlsbGluZy1jZmcuaWZzLmNsb3VkIiwiaHR0cHM6Ly9ib3JyZHJpbGxpbmctY2ZnLWxiLmlmcy5jbG91ZCJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiIsImRlZmF1bHQtcm9sZXMtYm9ycmNmZzEiXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6ImVtYWlsIGF1ZGllbmNlIHByb2ZpbGUgbWljcm9wcm9maWxlLWp3dCIsInNpZCI6ImM0NDc4ZmNmLWJhMDAtNGYyOS04Y2IzLWQ1NjBhOWNkYjU2ZSIsInVwbiI6Imlmc2Jvb21pIiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJjbGllbnRJZCI6IklGU19ib29taSIsImNsaWVudEhvc3QiOiIxMC4yMjcuNi4xMDQiLCJncm91cHMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiIsImRlZmF1bHQtcm9sZXMtYm9ycmNmZzEiXSwicHJlZmVycmVkX3VzZXJuYW1lIjoiaWZzYm9vbWkiLCJjbGllbnRBZGRyZXNzIjoiMTAuMjI3LjYuMTA0In0.gFYUmkSlOAQOx6r7A3WX_35J0LxvSXhAFQ6Ak2YOu-8LTI7p1-jIGvH2th6z3kvuVgfC2eEnrcpglKpJK-Q0epA-LFNhUBZfYzHd_TaV17UTfP7fncw6EEglMFKlft_fQDcw5dUlFK0O6kb9F865UTgsP0AP7x_5PE_SdlTdrzgbFEJTeBLHGgVUOzIjbmsQHWE57zpdJUBs-iKRwnov5UzUe0P8Kjc89wJizIUax_q03YcSJnC-TaHl2fIFsUaAaSSP9UgTZtsxG8i2jE0RUQuel1j-nC7Thv5IklEHn9U33Zy76DSY2TK52VibSU5UiDTIz-j_sxQ_tl3h0gkk_w";

        static async Task Main(string[] args)
        {
            var httpClient = new HttpClient();

            var values = new Dictionary<string, string>
            {
                { "grant_type", "client_credentials" },
                { "client_id", clientId },
                { "client_secret", clientSecret }
            };

            var content = new FormUrlEncodedContent(values);

            var response = await httpClient.PostAsync(tokenEndpoint, content);

            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine($"Token request failed: {response.StatusCode}");
                return;
            }

            var responseContent = await response.Content.ReadAsStringAsync();
            var doc = JsonDocument.Parse(responseContent);
            var accessToken = "";
            if (doc.RootElement.TryGetProperty("access_token", out var accessTokenElement))
            {
                accessToken = accessTokenElement.GetString();

                if (string.IsNullOrEmpty(accessToken))
                {
                    Console.WriteLine("Error, null token");
                    return;
                }
            }

            //Authorise using access token
            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);

            //Handle the request (?$filter etc)
            /*response = await httpClient.GetAsync(authEndpoint);
            responseContent = await response.Content.ReadAsStringAsync();

            Console.WriteLine("Status code: " + response.StatusCode.ToString());
            Console.WriteLine("Access token response:");
            Console.WriteLine(responseContent.Substring(0,1000));*/

            ODataClientSettings settings = new ODataClientSettings(new Uri(baseURI))
            {
                BeforeRequest = message =>
                {
                    message.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                }
            };
            ODataClient Client = new ODataClient(settings);

            RedudantTagDiscovery();

            //var allData = Test_TagDiscovery(Client);
            var result = Fetch(Client);

            Console.WriteLine();
            Console.WriteLine("Press any key to exit");
            Console.ReadLine();
        }

        public class AvailableTagDataObject
        {
            public decimal? Maximum;
            public decimal? Minimum;
            public string NativeName
            {
                get; set;
            }
            public string Description
            {
                get; set;
            }
            public string Units
            {
                get;
                set;
            }

            public AvailableTagDataObject(string nativeName)
            {
                NativeName = nativeName;
            }
        }

        public class GetAvailableTagsRequest
        {
            public void AddDataObject(object o)
            {

            }
        }

        public static void RedudantTagDiscovery()
        {
            /*IEnumerable<dynamic> allData;
            try
            {
                var metadata = await Client.GetMetadataAsync();

                var annotations = new ODataFeedAnnotations();

                var startTime = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
                var startTimeString = startTime.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ssZ");

                var uri = baseURI + "/" + TableName + $"?$filter=CreatedDate gt {startTimeString}";

                allData = Client.FindEntriesAsync(uri).Result.ToArray();
                var totalCount = annotations.Count;

                Console.WriteLine(allData);
                Console.WriteLine("TotalCount: " + totalCount);
            }
            catch (ReflectionTypeLoadException ex)
            {
                foreach (var loaderException in ex.LoaderExceptions)
                {
                    Console.WriteLine("Loader exception: " + loaderException.Message);
                }
            }
            catch (Simple.OData.Client.WebRequestException ex)
            {
                Console.WriteLine("WebRequestException caught!");
                Console.WriteLine($"Status Code: {ex.Code}");            // e.g., 400, 401, 500
                Console.WriteLine($"Message: {ex.Message}");
                Console.WriteLine($"Response (raw body): {ex.Response}"); // Often includes error details in JSON or XML

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Inner Exception: {ex.InnerException.GetType().Name}");
                    Console.WriteLine($"Inner Exception Message: {ex.InnerException.Message}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }*/
        }

        public static IEnumerable Test_TagDiscovery(ODataClient OdataClient)
        {
            GetAvailableTagsRequest request = new GetAvailableTagsRequest();
            var batchSize = 20000;
            Dictionary<string, AvailableTagDataObject> availableTags = new Dictionary<string, AvailableTagDataObject>();
            DataTable tagTable = null;
            var today = DateTime.Today;
            var scanDateTimeStr = today.AddDays(-100).ToString("yyyy-MM-ddTHH:mm:ss");
            var todayDateTimeStr = today.ToString("yyyy-MM-ddTHH:mm:ss");

            //oData queries do not seem to support IN or wildcard - this may be problematic where there are a large number of tags - recommendation in such cases would be that the customer expose a discovery endpoint
            string filter = $"{TimeStampField} gt datetime'{scanDateTimeStr}' and {TimeStampField} le datetime'{todayDateTimeStr}'";

            IEnumerable<dynamic> allData = null;

            var fetchCount = 0;
            var count = 0;
            while (fetchCount == 0 || (allData != null && allData.Count() >= batchSize)) //when there's less records than our batchSize - we know there are no more records.
            {
                ++fetchCount;

                var annotations = new ODataFeedAnnotations();
                var query = OdataClient
                .For(TableName)
                //.Filter(filter)
                .OrderByDescending(TimeStampField);

                var queryString = $"{baseURI}/{TableName}?$filter={filter}&$orderby={TimeStampField} desc";
                if (batchSize > 0)
                {
                    query = query.Skip(count).Top(batchSize);
                    queryString += $"&$skip={count}&$top={batchSize}";
                }

                Console.WriteLine($"Querying: {queryString}");

                allData = query.FindEntriesAsync(annotations).Result;
                var totalCount = annotations.Count;

                if (allData != null && allData.Any())
                {
                    count += allData.Count();
                    Console.WriteLine("Discovered {0} records of {1} on fetch {2} with query {3}{4}{5}", allData.Count(), totalCount, fetchCount, queryString, Environment.NewLine, "note: for the full record count include '&$inlinecount=allpages' in the query above");
                    tagTable = ConvertToDataTable(allData);

                    var GroupBy = tagTable.AsEnumerable().
                                        GroupBy(e => e.Field<string>(NameField) ?? "NULL")
                                        .Select(d => new
                                        {
                                            d.Key,
                                            Count = d.Count()
                                        });

                    foreach (var data in GroupBy)
                    {
                        var results = (from myRow in tagTable.AsEnumerable()
                                       where myRow.Field<string>(NameField) == data.Key
                                       select myRow);
                        foreach (var col in ListColumns)
                        {
                            if ((NameField.Contains(col) == false) && (TimeStampField.Contains(col) == false))
                            {
                                CheckForDatatype(availableTags, results, request, data.Key, col);
                            }
                        }
                    }
                }
                else
                {
                    allData = null;
                    break; // if we don't have data - we need to quit the while loop
                }
                if (batchSize == 0)
                {
                    break;
                }
            }
            foreach (KeyValuePair<string, AvailableTagDataObject> kvp in availableTags)
            {
                if (kvp.Value != null)
                {
                    request.AddDataObject(kvp.Value);
                    Console.WriteLine("Tag Added: " + kvp.Key + " Max: " + kvp.Value.Maximum + " Min: " + kvp.Value.Minimum);
                }
                else
                {
                    Console.WriteLine($"Failed to add Tag: {kvp.Key}");
                }
            }
            return allData;
        }

        /// <summary>
        /// Converts the results of tag discovery (where no discovery endpoint exists) into a data table for processing.
        /// </summary>
        /// <param name="allData"></param>
        /// <param name="tagTable"></param>
        private static DataTable ConvertToDataTable(IEnumerable<dynamic> allData)
        {
            Console.WriteLine("--------ConvertToDataTable --------");
            DataTable tagTable = new DataTable();

            if (allData != null)
            {
                if (tagTable.Columns.Count == 0)
                {
                    int count = 10;
                    int[] index = new int[count];
                    var json = JsonConvert.SerializeObject(allData);
                    tagTable = (DataTable)JsonConvert.DeserializeObject(json, (typeof(DataTable)));
                }
            }
            tagTable.DefaultView.Sort = NameField + " desc";
            tagTable = tagTable.DefaultView.ToTable();

            var cols = (from dc in tagTable.Columns.Cast<DataColumn>()
                        select dc.ColumnName).ToArray();
            foreach (var columnName in cols)
            {
                if ((NameField.Contains(columnName) == false) && (TimeStampField.Contains(columnName) == false) &&
                        (ListColumns.Contains(columnName) == false))
                {
                    tagTable.Columns.Remove(columnName);
                }
            }
            return tagTable;
        }

        /// <summary>
        /// Checks the datatypes when performing tag discovery where a discovery endpoint isn't available
        /// </summary>
        /// <param name="tagTable"></param>
        /// <param name="request"></param>
        /// <param name="key"></param>
        /// <param name="column"></param>
        /// <returns></returns>
        public static bool CheckForDatatype(Dictionary<string, AvailableTagDataObject> availableTags, EnumerableRowCollection<DataRow> tagTable, GetAvailableTagsRequest request, string key, string column)
        {
            //object avg = null;
            object min = null;
            object max = null;
            try
            {
                var gvs = tagTable.AsEnumerable()
                  .Where(row => row.Field<string>(NameField) == key)
                  .Select(row => row.Field<decimal>(column)).ToArray();

                //avg = (from a in gvs select a).Average();
                min = (from a in gvs select a).Min();
                max = (from a in gvs select a).Max();
            }
            catch (Exception e)
            {
                try
                {
                    var gvs = tagTable.AsEnumerable()
                      .Where(row => row.Field<string>(NameField) == key)
                      .Select(row => row.Field<double?>(column) ?? 0.0).ToArray();

                    //avg = (from a in gvs select a).Average();
                    min = (from a in gvs select a).Min();
                    max = (from a in gvs select a).Max();
                }
                catch (Exception e2)
                {
                    try
                    {
                        var gvs = tagTable.AsEnumerable()
                          .Where(row => row.Field<string>(NameField) == key)
                          .Select(row => row.Field<int?>(column) ?? 0).ToArray();

                        //avg = (from a in gvs select a).Average();
                        min = (from a in gvs select a).Min();
                        max = (from a in gvs select a).Max();

                    }
                    catch (Exception e3)
                    {
                        try
                        {
                            var gvs = tagTable.AsEnumerable()
                                        .Where(row => row.Field<string>(NameField) == key)
                                            .Select(row => row.Field<string>(column) ?? "").ToArray();
                            try
                            {
                                var gvs2 = Array.ConvertAll<string, decimal>(gvs, Convert.ToDecimal);
                                min = (from a in gvs2 select a).Min();
                                max = (from a in gvs2 select a).Max();
                            }
                            catch (Exception e4) { } // we don't return false - we simply don't set mex/min
                        }
                        catch (Exception e4)
                        {
                            return false; // if we get here we haven't been able to get data for this tag
                        }
                    }
                }
            }

            try
            {
                decimal maximum = decimal.MaxValue;
                decimal minimum = decimal.MinValue;
                bool maxSet = false;
                bool minSet = false;
                AvailableTagDataObject tagDataObject = null;
                if (max != null)
                {
                    maxSet = decimal.TryParse(max.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out maximum);
                }
                if (min != null)
                {
                    minSet = decimal.TryParse(min.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out minimum);
                }

                string nativeName = WideMode ? key + Delimiter + column : key;
                //if this tag already exists - it may have been created in an earlier batch. we still need to attempt to set min/max as they may not be accurate
                if (!availableTags.Keys.Contains(nativeName, StringComparer.CurrentCultureIgnoreCase))
                {
                    tagDataObject = new AvailableTagDataObject(nativeName)
                    {
                        Description = nativeName + " EnergySys oData tag",
                        Units = "tbd"
                    };
                    availableTags.Add(nativeName, tagDataObject);
                }
                else
                {
                    tagDataObject = availableTags[nativeName];
                }
                if (maxSet)
                {
                    tagDataObject.Maximum = tagDataObject.Maximum.HasValue ? Math.Max(tagDataObject.Maximum.Value, maximum) : maximum;
                }
                if (minSet)
                {
                    tagDataObject.Minimum = tagDataObject.Minimum.HasValue ? Math.Min(tagDataObject.Minimum.Value, minimum) : minimum;
                }
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        /// <summary>
        /// Batches queries for the odata endpoint to process
        /// </summary>
        /// <param name="request"></param>
        /// <param name="entityNames"></param>
        /// <returns></returns>
        public static async Task<List<dynamic>> BatchQuery(ODataClient Client, GetTagDataRequest request, List<string> entityNames, List<string> attributeNames)
        {
            bool attributeMode = false;
            List<IEnumerable<dynamic>> batchRequests = new List<IEnumerable<dynamic>>();
            var startTime = request.StartTime.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture);
            var endTime = request.EndTime.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture);
            var batches = new ODataBatch(Client);

            Dictionary<int, List<string>> batchIndex = new Dictionary<int, List<string>>();

            StringBuilder verboseLog = new StringBuilder("Batch querying Odata for the following queries:");

            if (request.DataObjects.Count > 0)
            {
                int batchCount = 0;
                int processedCount = 0;
                int entitiesCount = 0;
                while (processedCount < entityNames.Count)
                {
                    var batchEntityNames = 20000 > 0 ? entityNames.Skip(processedCount).Take(20000).ToList() : entityNames.ToList();
                    string filter = "";
                    string entitiesFilter = "";
                    foreach (string entity in batchEntityNames)
                    {
                        entitiesFilter += $"{NameField} eq '{entity}' or ";
                        ++entitiesCount;
                    }
                    entitiesFilter = entitiesFilter.Substring(0, entitiesFilter.Length - 3);


                    List<string> attributes = new List<string>();

                    if (WideMode)
                    {
                        attributes.Add(NameField);
                        attributes.Add(TimeStampField);
                        /*if (!string.IsNullOrWhiteSpace("Confidence"))
                        {
                            attributes.Add("Confidence");
                        }*/

                foreach (string attribute in attributeNames)
                        {
                            attributes.Add(attribute);
                        }
                    }
                    verboseLog.Append(Environment.NewLine);

                    IBoundClient<IDictionary<string, object>> batchRequest;
                    string uri;
                    uri = $"{baseURI}/{TableName}?$filter={{0}}&$orderby={TimeStampField}";
                    if (request.StartTime != request.EndTime)
                    {
                        filter = $"{TimeStampField} le datetime'{endTime}' and {TimeStampField} gt datetime'{startTime}' and ({entitiesFilter})";
                        batchRequest = Client
                            .For(TableName)
                            //.Filter(filter)
                            .OrderBy(TimeStampField);
                    }
                    else
                    {
                        filter = $"{TimeStampField} le datetime'{endTime}' and ({entitiesFilter})";
                        batchRequest = Client
                            .For(TableName)
                            //.Filter(filter)
                            .OrderByDescending(TimeStampField)
                            .Top(batchEntityNames.Count);
                        uri += $" desc&$top={batchEntityNames.Count}";
                    }
                    if (WideMode)
                    {
                        batchRequest = batchRequest.Select(attributes);
                        uri += $"&$select={string.Join(",", attributes)}";
                    }

                    batches += async c => batchRequests.Add(await batchRequest.FindEntriesAsync().ConfigureAwait(true));
                    verboseLog.Append(String.Format(uri, filter));

                    batchIndex.Add(batchCount, batchEntityNames);
                    ++batchCount;

                    //Logger.Verbose(verboseLog.ToString());
                    processedCount += batchEntityNames.Count;
                }
            }
            else
            {
                Console.WriteLine("No tags were queried.");
                //request.Fail(new InvalidOperationException("No Results returned from query."));
                throw new ArgumentNullException(nameof(request));
            }

            try
            {
                await batches.ExecuteAsync().ConfigureAwait(true);
            }
            catch (WebRequestException ex)
            {
                return null;// HandleOdataBadResponse(request, ex);
            }
            catch (Exception ex)
            {
                return null;// HandleException(request, ex);
            }

            List<dynamic> result = new List<dynamic>();
            int i = 0;
            GetTagDataRequest singlePointSpotFetch = null;
            foreach (var retVal in batchRequests)
            {
                result.AddRange(retVal);
                ++i;
            }

            Console.WriteLine("Successfully Queried Odata for single point values for entities {0}", string.Join(",", entityNames));
            return result;
        }

        private void Fetch(GetTagDataRequest request)
        {
            try
            {
                ConnectODataServer();

                List<string> entityNames;
                List<string> attributeNames = new List<string>();
                List<dynamic> allData;

                if (Settings.WideMode)
                {
                    GetEntityAndAttributeNames(request, out entityNames, out attributeNames);
                }
                else
                {
                    entityNames = request.DataObjects.Select(x => x.NativeName).ToList();
                }
                allData = BatchQuery(request, entityNames, attributeNames).Result;

                var valuesForTag = ProcessOdataResponse(request, allData);
                ProcessReturnData(request, valuesForTag);

                //if we had a bad attribute we need to redo the fetch as we simply get a response back that would block all results from being returned
                //unfortunately we don't get back a list of errors, but rather one at a time.
                int badAttributes = 0;
                bool badAttributeAdded = false;
                if (BadAttributes.Count > 0)
                {
                    badAttributes = BadAttributes.Count;
                    badAttributeAdded = true;
                }
                while (badAttributeAdded)
                {
                    allData.Clear();
                    badAttributeAdded = false;
                    RedoRequest(request, ref entityNames, ref attributeNames, ref valuesForTag);
                    if (BadAttributes.Count > badAttributes)
                    {
                        badAttributes = BadAttributes.Count;
                        badAttributeAdded = true;
                    }
                }

                //if there was no data in a single point fetch it was dropped from the response because the top results weren't sufficient
                //if there was only one data object and it was a single point fetch - we don't need to perform a rerun.
                if (request.DataObjects.Count > 1 && request.EndTime == request.StartTime
                    && request.DataObjects.Any(x => x.Value.Type != VariableType.Error && x.Value.Collection.Count == 0))
                {
                    RedoRequest(request, ref entityNames, ref attributeNames, ref valuesForTag, request.StartTime);

                    //if we still have empty records we'll need to handle these individually. by this stage there should not be too many tags left unpopulated
                    if (request.DataObjects.Any(x => x.Value == null || (x.Value.Type != VariableType.Error && x.Value.Collection.Count == 0)))
                    {
                        Dictionary<string, List<GetTagDataObject>> entitiesWithNoDatums = new Dictionary<string, List<GetTagDataObject>>();
                        //group by entityName to minimize requests
                        foreach (var dataObject in request.DataObjects.Where(x => x.Value == null || (x.Value.Type != VariableType.Error && x.Value.Collection.Count == 0)))
                        {
                            if (dataObject?.NativeName.IndexOf(Settings.Delimiter, StringComparison.CurrentCultureIgnoreCase) > 0)
                            {
                                var entityName = Settings.WideMode ? dataObject.NativeName.Split(Settings.Delimiter.ToCharArray())[0] : dataObject.NativeName;
                                if (!entitiesWithNoDatums.ContainsKey(entityName))
                                {
                                    entitiesWithNoDatums.Add(entityName, new List<GetTagDataObject>());
                                }
                                entitiesWithNoDatums[entityName].Add(dataObject);
                            }
                        }

                        foreach (string entityName in entitiesWithNoDatums.Keys)
                        {
                            GetTagDataRequest noDataSoRedoRequest = new GetTagDataRequest(request.StartTime, request.EndTime, request.SampleMethod, request.SampleInterval);
                            entitiesWithNoDatums[entityName].ForEach(dataObject => noDataSoRedoRequest.AddDataObject(dataObject.Name, dataObject.NativeName));

                            RedoRequest(noDataSoRedoRequest, ref entityNames, ref attributeNames, ref valuesForTag, request.StartTime);

                            foreach (var retObj in noDataSoRedoRequest.DataObjects)
                            {
                                var requestObj = request.GetDataObject(retObj.NativeName);
                                if (retObj.Value != null)
                                {
                                    requestObj.Value = retObj.Value;
                                }
                            }
                        }
                    }
                }

                //if there was no data in a historic fetch - we need to do a single point spot fetch - the redo request will only perform a query if it finds a dataObject with no data.
                if (request.EndTime != request.StartTime)
                {
                    RedoRequest(request, ref entityNames, ref attributeNames, ref valuesForTag, request.StartTime);
                }
            }
            catch (Exception e)
            {
                HandleException(request, e);
                return;
            }
        }

        /*public static Dictionary<string,List<VariableValue>> Fetch(ODataClient Client)
        {
            Dictionary<string, List<VariableValue>> result = null;
            try
            {

                GetTagDataRequest request = new GetTagDataRequest();
                request.DataObjects.Add(new GetTagDataObject("DATA_SYNC_IN_LOB_SA13.SeqNo"));
                request.DataObjects.Add(new GetTagDataObject("DATA_SYNC_IN_LOB_SA13.State"));
                request.StartTime = Instant.FromDateTimeUtc(DateTime.UtcNow.AddDays(-100));
                request.EndTime = Instant.FromDateTimeUtc(DateTime.UtcNow);
                request.SampleInterval = Instant.FromUnixTimeSeconds(60);
                request.SampleMethod = 0;

                List<string> entityNames;
                List<string> attributeNames = new List<string>();

                if (WideMode)
                {
                    GetEntityAndAttributeNames(request, out entityNames, out attributeNames);
                }
                else
                {
                    entityNames = request.DataObjects.Select(x => x.NativeName).ToList();
                }
                List<dynamic> allData = BatchQuery(Client, request, entityNames, attributeNames).Result;

                var valuesForTag = ProcessOdataResponse(request, allData);
                //works up to here

                if (request.EndTime != request.StartTime)
                {
                    RedoRequest(Client, request, ref entityNames, ref attributeNames, ref valuesForTag, request.StartTime);
                }

                result = valuesForTag;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return null;
            }

            return result;
        }*/

        /*
        //if there was no data in a single point fetch it was dropped from the response because the top results weren't sufficient
                //if there was only one data object and it was a single point fetch - we don't need to perform a rerun.
                if (request.DataObjects.Count > 1 && request.EndTime == request.StartTime
                    && request.DataObjects.Any(x => x.Value.Type() != "VariableType.Error" && x.Value.Collection.Count == 0))
                {
                    RedoRequest(Client, request, ref entityNames, ref attributeNames, ref valuesForTag, request.StartTime);

                    //if we still have empty records we'll need to handle these individually. by this stage there should not be too many tags left unpopulated
                    if (request.DataObjects.Any(x => x.Value == null || (x.Value.Type() != "VariableType.Error" && x.Value.Collection.Count == 0)))
                    {
                        Dictionary<string, List<GetTagDataObject>> entitiesWithNoDatums = new Dictionary<string, List<GetTagDataObject>>();
                        //group by entityName to minimize requests
                        foreach (var dataObject in request.DataObjects.Where(x => x.Value == null || (x.Value.Type() != "VariableType.Error" && x.Value.Collection.Count == 0)))
                        {
                            if (dataObject?.NativeName.IndexOf(Delimiter, StringComparison.CurrentCultureIgnoreCase) > 0)
                            {
                                var entityName = WideMode ? dataObject.NativeName.Split(Delimiter.ToCharArray())[0] : dataObject.NativeName;
                                if (!entitiesWithNoDatums.ContainsKey(entityName))
                                {
                                    entitiesWithNoDatums.Add(entityName, new List<GetTagDataObject>());
                                }
                                entitiesWithNoDatums[entityName].Add(dataObject);
                            }
                        }

                        foreach (string entityName in entitiesWithNoDatums.Keys)
                        {
                            GetTagDataRequest noDataSoRedoRequest = new GetTagDataRequest(request.StartTime, request.EndTime, request.SampleMethod, request.SampleInterval);
                            entitiesWithNoDatums[entityName].ForEach(dataObject => noDataSoRedoRequest.AddDataObject(dataObject.Name, dataObject.NativeName));

                            RedoRequest(Client, noDataSoRedoRequest, ref entityNames, ref attributeNames, ref valuesForTag, request.StartTime);

                            foreach (var retObj in noDataSoRedoRequest.DataObjects)
                            {
                                var requestObj = request.GetDataObject(retObj.NativeName);
                                if (retObj.Value != null)
                                {
                                    requestObj.Value = retObj.Value;
                                }
                            }
                        }
                    }
                }

                //if there was no data in a historic fetch - we need to do a single point spot fetch - the redo request will only perform a query if it finds a dataObject with no data.
                if (request.EndTime != request.StartTime)
                {
                    RedoRequest(Client, request, ref entityNames, ref attributeNames, ref valuesForTag, request.StartTime);
                }
                return valuesForTag;
            }
			catch (Exception e)
			{
                //HandleException(request, e);
                Console.WriteLine("Exception: {0}", e);
				return null;
			}
            return null; 
         */

        /// <summary>
        /// where incomplete result sets occur (eg: there are multiple records returned for some tags on the first batch that push other tags into another batch) we need to redo the request, removing tags that we have results for
        /// terribly inefficient - but odata doesn't provide a means to query 'in' a list of tag names, so we need to keep scraping results from the top until we get all our tag datums
        /// </summary>
        /// <param name="request"></param>
        /// <param name="entityNames"></param>
        /// <param name="attributeNames"></param>
        /// <param name="valuesForTag"></param>
        /// <param name="requestEndTime"></param>
        public static void RedoRequest(ODataClient Client, GetTagDataRequest request, ref List<string> entityNames, ref List<string> attributeNames, ref Dictionary<string, List<VariableValue>> valuesForTag, Instant? requestEndTime = null)
        {
            if (!requestEndTime.HasValue)
            {
                requestEndTime = request.EndTime;
            }

            GetTagDataRequest noDataSoRedoRequest = new GetTagDataRequest(request.StartTime, requestEndTime.Value, request.SampleMethod, request.SampleInterval);
            foreach (var dataObject in request.DataObjects)
            {
                if (dataObject.Value == null || (dataObject.Value.Type() != "VariableType.Error" && dataObject.Value.Collection.Count == 0))
                {
                    noDataSoRedoRequest.AddDataObject(dataObject.Name, dataObject.NativeName);
                }
            }

            if (noDataSoRedoRequest.DataObjects.Any())
            {
                List<dynamic> allData;
                if (WideMode)
                {
                    GetEntityAndAttributeNames(noDataSoRedoRequest, out entityNames, out attributeNames);
                }
                allData = BatchQuery(Client, noDataSoRedoRequest, entityNames, attributeNames).Result;
                valuesForTag = ProcessOdataResponse(noDataSoRedoRequest, allData);
                //when processing the return data we reapply the original endTime so that if it was a historic fetch we correctly populate the sample interval data points.	
                ProcessReturnData(Client, noDataSoRedoRequest, valuesForTag, request.EndTime);

                foreach (var dataObject in noDataSoRedoRequest.DataObjects)
                {
                    request.DataObjects.First(x => x.Name == dataObject.Name).Value = dataObject.Value;
                }
            }
        }

        /// <summary>
        /// Once we have variable values - attach them to the p2 response object
        /// </summary>
        /// <param name="request"></param>
        /// <param name="valuesForTag"></param>
        /// <param name="requestEndTime">end time is required for redo-requests for missing datapoints is a single point fetch, but may be from a historic fetch.</param>
        public static void ProcessReturnData(ODataClient Client, GetTagDataRequest request, Dictionary<string, List<VariableValue>> valuesForTag, Instant? requestEndTime = null)
        {
            Console.WriteLine("--------ProcessReturnData--------");
            if (!requestEndTime.HasValue)
            {
                requestEndTime = request.EndTime;
            }

            foreach (GetTagDataObject dataObject in request.DataObjects)
            {
                if (dataObject.Value != null) //check for null as bad responses will have already populated the dataobject with an error
                {
                    continue;
                }
                List<VariableValue> vals;
                if (valuesForTag.TryGetValue(dataObject.NativeName, out vals))
                {
                    //NOTE: if we're doing a historic spot fetch to find a data point - we need to populate the sample interval data points.
                    if (vals.Count == 1 && request.StartTime != requestEndTime)
                    {
                        var sampleInterval = request.StartTime;
                        var val = vals[0];
                        if (val.Type() != "VariableValueType.Error")
                        {
                            while (sampleInterval <= requestEndTime)
                            {
                                val.Timestamp = sampleInterval;
                                vals.Add(val);
                                //sampleInterval += request.SampleInterval;
                            }
                        }
                    }

                    if (vals.Any(x => x.Type() == "VariableValueType.String"))
                    {
                        //any strings will result in the whole collection needing to be a string.
                        List<VariableValue> vvList = new List<VariableValue>();

                        if (vals.Any(x => x.Type() == "VariableValueType.Decimal"))
                        {
                            foreach (var val in vals)
                            {
                                if (val.Type() == "VariableValueType.Decimal")
                                {
                                    vvList.Add(new VariableValue(val.DecimalValue.ToString()));
                                }
                                else
                                {
                                    vvList.Add(val);
                                }
                            }
                        }
                        else
                        {
                            vvList.AddRange(vals);
                        }
                        //dataObject.Value = new VariableValueCollection(VariableValueCollection.ConvertToCollectionType("String"), vvList);
                    }
                    else // the only other data types we should handle are decimal - nulls and errors can be attached to this return type.
                    {
                        //dataObject.Value = new VariableValueCollection(VariableValueCollection.ConvertToCollectionType("Decimal"), vals);
                    }
                }
                else
                {
                    dataObject.Value = new VariableValue(0);//new VariableValueCollection("Unspecified", new List<dynamic>());
                }
            }
            Console.WriteLine("--------ProcessedReturnData--------");
        }

        public class VariableValueCollection
        {
            public List<dynamic> Collection = new List<dynamic>();
            public string type = "";
            public VariableValueCollection(string type, List<dynamic> vals)
            {
                this.type = type;
                this.Collection = vals;
            }
            public void ConvertToCollectionType(string type, List<VariableValue> vals)
            {
                this.type = type;
                this.Collection = vals.ToList<dynamic>();
            }
        }

        public class VariableValue
        {
            public Instant Timestamp { get; set; }
            public int Confidence { get; set; }
            public dynamic val { get; private set; }
            public string type { get; set;  }

            public List<dynamic> Collection { get; set; }
            public decimal DecimalValue = -1;

            public VariableValue(int first, string second)
            {
                if (first == -1 && string.IsNullOrEmpty(second))
                    val = null;
            }
            public VariableValue(Exception e)
            {
                this.val = e;
            }
            public VariableValue(decimal val)
            {
                this.val = val;
                DecimalValue = val;
                type = "decimal";
            }
            public VariableValue(double val)
            {
                this.val = val;
                type = "double";
            }
            public VariableValue(int val)
            {
                this.val = val;
                type = "int";
            }
            public VariableValue(string val)
            {
                this.val = val;
                type = "string";
            }

            public void CreateNull()
            {
                val = null;
            }

            public string Type()
            {
                return val.GetType();
            }
        }

        public class GetTagDataRequest
        {
            public List<GetTagDataObject> DataObjects = new List<GetTagDataObject>();
            public string NativeName { get; set; }
            public Instant StartTime { get; set; }
            public Instant EndTime { get; set; }
            public VariableValue Value { get; set; }
            public int SampleMethod { get; set; }
            public Instant SampleInterval { get; set; }
            public GetTagDataRequest(Instant s, Instant e, int m, Instant i)
            {
                this.StartTime = s;
                this.EndTime = e;
                this.SampleMethod = m;
                this.SampleInterval = i;
            }
            public GetTagDataRequest()
            {

            }
            public void AddDataObject(string name, string nativeName)
            {
                DataObjects.Add(new GetTagDataObject(name));
            }
            public GetTagDataObject GetDataObject(string name)
            {
                return DataObjects.First(x => x.Name == name);
            }
        }

        public class GetTagDataObject
        {
            public string Name { get; set; }
            public string NativeName { get; set; }
            public VariableValue Value { get; set; }

            public GetTagDataObject(string name)
            {
                NativeName = name;
            }
        }

        /// <summary>
        /// process the odata response and translate it into variable values
        /// </summary>
        /// <param name="request"></param>
        /// <param name="odataResponse"></param>
        /// <param name="tagRootList"></param>
        /// <param name="tagList"></param>
        public static Dictionary<string, List<VariableValue>> ProcessOdataResponse(GetTagDataRequest request, IEnumerable<dynamic> odataResponse)
        {
            Dictionary<string, List<VariableValue>> valuesForTag = new Dictionary<string, List<VariableValue>>(StringComparer.InvariantCultureIgnoreCase);

            if (odataResponse == null || !odataResponse.Any())
            {
                List<string> tagNames = new List<string>();
                foreach (var dataObject in request.DataObjects)
                {
                    tagNames.Add(dataObject.NativeName);
                }
                Console.WriteLine("No data was returned for the tags {0}", string.Join(", ", tagNames));
            }
            else
            {
                try
                {
                    foreach (Dictionary<string, object> retVal in odataResponse)
                    {
                        string name = (string)retVal[NameField];
                        DateTime timestampDT;
                        try
                        {
                            timestampDT = ((DateTime)retVal[TimeStampField]).ToUniversalTime();
                        }
                        catch (Exception ex)
                        {
                            try
                            {
                                timestampDT = DateTime.Parse(retVal[TimeStampField].ToString()).ToUniversalTime();
                            }
                            catch (Exception ex2)
                            {
                                //Logger.Error("","Error casting TimeStampField to DateTime");
                                continue;
                            }
                        }
                        int confidence = 100;
                        if (!string.IsNullOrWhiteSpace("Confidence") && retVal.ContainsKey("Confidence") && !int.TryParse(retVal["Confidence"].ToString(), out confidence))
                        {
                            Console.WriteLine($"Unable to load confidence for entity: {name} at timestamp: {timestampDT}");
                        }
                        Instant timestamp = Instant.FromDateTimeUtc(timestampDT);
                        //in wide mode we may have multiple columns returned for the queried entity, so we need to loop through the remaining results
                        //in narrow mode we just have value, but this code works just fine
                        foreach (KeyValuePair<string, object> tagValue in retVal.Where(x => ListColumns.Contains(x.Key) && x.Key != NameField && x.Key != TimeStampField && x.Key != "Confidence"))
                        {
                            VariableValue vv = null;
                            string tagName = null;
                            if (WideMode)
                            {
                                tagName = String.Join(Delimiter, name, tagValue.Key);
                            }
                            else
                            {
                                tagName = name;
                            }


                            //the dynamic object returns the correct datatype, so we can just switch on the value
                            //NOTE: we're only expecting to deal with strings and decimals. This can be expanded later, but would require changes to ProcessReturnData()
                            switch (tagValue.Value)
                            {
                                case double val:
                                    vv = new VariableValue(val);
                                    break;
                                case decimal val:
                                    vv = new VariableValue(val);
                                    break;
                                case int val:
                                    vv = new VariableValue((decimal)val);
                                    break;
                                case bool val:
                                    vv = new VariableValue(val ? 1.0 : 0.0);
                                    break;
                                case string val:
                                    //when in narrow mode - strings may be mixed in with decimal/integer
                                    if (!WideMode)
                                    {
                                        decimal tryDecimal = 0;
                                        int tryInt = 0;
                                        if (decimal.TryParse(val, out tryDecimal))
                                        {
                                            vv = new VariableValue(tryDecimal);
                                        }
                                        else if (int.TryParse(val, out tryInt))
                                        {
                                            vv = new VariableValue(tryInt);
                                        }
                                        break;
                                    }
                                    vv = new VariableValue(val);
                                    break;
                                case DateTime val:
                                    vv = new VariableValue(Instant.FromDateTimeUtc(val).ToString());
                                    break;
                                case null:
                                    vv = new VariableValue(-1,null);
                                    break;
                                default:
                                    vv = new VariableValue(new Exception("Unsupported data type."));
                                    break;
                            }

                            vv.Timestamp = timestamp;
                            vv.Confidence = confidence;

                            if (
                                valuesForTag.TryGetValue(tagName, out var throwAway))
                            {
                                if (request.StartTime != request.EndTime) //if this is a single point fetch but we got back multiple values - we only add the latest value.
                                {
                                    valuesForTag[tagName].Add(vv);
                                }
                            }
                            else
                            {
                                valuesForTag.Add(tagName, new List<VariableValue>() { vv });
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    return null;
                }
            }
            return valuesForTag;
        }

        /// <summary>
        /// For wide configuration - take the tag names and split them into entity (name) and attribute (column)
        /// </summary>
        /// <param name="request"></param>
        /// <param name="entityNames"></param>
        /// <param name="tagList"></param>
        public static void GetEntityAndAttributeNames(GetTagDataRequest request, out List<string> entityNames, out List<string> attributeNames)
        {
            entityNames = new List<string>();
            attributeNames = new List<string>();
            foreach (var dataObject in request.DataObjects)
            {
                var tagName = dataObject.NativeName;
                if (!tagName.Contains(Delimiter))
                {
                    var error = $"The tag {tagName} does not contain the Delimiting Character ({Delimiter}). tags must conform to the convention [entity]{Delimiter}[attribute] when querying Odata.";
                    Console.WriteLine(error);
                    //attach the error to the queried tag
                    var tag = request.DataObjects.FirstOrDefault(x => x.NativeName == tagName);
                    if (tag != null)
                    {
                        //tag.Value = new Variable(new ErrorInfo(new Exception(error)));
                        throw new Exception(error);
                    }
                    continue;
                }
                string[] entity = tagName.Split(new string[] { Delimiter }, StringSplitOptions.None);
                if (!entityNames.Contains(entity[0]))
                {
                    entityNames.Add(entity[0]);
                }
                if (!attributeNames.Contains(entity[1]))// && !BadAttributes.Contains(entity[1]))
                {
                    attributeNames.Add(entity[1]);
                }
            }
        }
    }
}
