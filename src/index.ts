import { Root } from "./response";
import fs from "fs";

const responseBodyOutputFile: string | null = "responseOut.json";
const dataOutputFile: string | null = "dataOut.json";

const summaryIndex = "gracc.osg.summary";
const endpoint = "https://gracc.opensciencegrid.org:443/q";

type JobDataPoint = {
  id: string;
  projectName: string;
  cpuHours: number;
  nJobs: number;
  endTime: string;
};

// Parse an ISO date string (i.e. "2019-01-18T00:00:00.000Z",
// "2019-01-17T17:00:00.000-07:00", or "2019-01-18T07:00:00.000+07:00",
// which are the same time) and return a JavaScript Date object with the
// value represented by the string.
function isoStringToDate(isoString: string): Date {
  // Split the string into an array based on the digit groups.
  var dateParts = isoString.split(/\D+/);

  // Set up a date object with the current time.
  var returnDate = new Date();

  // Manually parse the parts of the string and set each part for the
  // date. Note: Using the UTC versions of these functions is necessary
  // because we're manually adjusting for time zones stored in the
  // string.
  returnDate.setUTCFullYear(parseInt(dateParts[0]));

  // The month numbers are one "off" from what normal humans would expect
  // because January == 0.
  returnDate.setUTCMonth(parseInt(dateParts[1]) - 1);
  returnDate.setUTCDate(parseInt(dateParts[2]));

  // Set the time parts of the date object.
  returnDate.setUTCHours(parseInt(dateParts[3]));
  returnDate.setUTCMinutes(parseInt(dateParts[4]));
  returnDate.setUTCSeconds(parseInt(dateParts[5]));
  returnDate.setUTCMilliseconds(parseInt(dateParts[6]));

  // Track the number of hours we need to adjust the date by based
  // on the timezone.
  var timezoneOffsetHours = 0;

  // If there's a value for either the hours or minutes offset.
  if (dateParts[7] || dateParts[8]) {
    // Track the number of minutes we need to adjust the date by
    // based on the timezone.
    var timezoneOffsetMinutes = 0;

    // If there's a value for the minutes offset.
    if (dateParts[8]) {
      // Convert the minutes value into an hours value.
      timezoneOffsetMinutes = parseInt(dateParts[8]) / 60;
    }

    // Add the hours and minutes values to get the total offset in
    // hours.
    timezoneOffsetHours = parseInt(dateParts[7]) + timezoneOffsetMinutes;

    // If the sign for the timezone is a plus to indicate the
    // timezone is ahead of UTC time.
    if (isoString.substr(-6, 1) == "+") {
      // Make the offset negative since the hours will need to be
      // subtracted from the date.
      timezoneOffsetHours *= -1;
    }
  }

  // Get the current hours for the date and add the offset to get the
  // correct time adjusted for timezone.
  returnDate.setHours(returnDate.getHours() + timezoneOffsetHours);

  // Return the Date object calculated from the string.
  return returnDate;
}

async function graccQuery(
  start: string | Date,
  end: string | Date
): Promise<JobDataPoint[] | null> {
  const startStr = typeof start === "string" ? start : start.toISOString();
  const endStr = typeof end === "string" ? end : end.toISOString();

  /*
  s = Search(using=es, index=index)

    s = s.query('bool',
            filter=[
             Q('range', EndTime={'gte': starttime, 'lt': endtime })
          &  Q('term',  ResourceType='Batch')
          & ~Q('terms', SiteName=['NONE', 'Generic', 'Obsolete'])
          & ~Q('terms', VOName=['Unknown', 'unknown', 'other'])
        ]
    )

    if offset is None:
        extra = {}
    else:
        extra = {'offset': "-%ds" % offset}

    curBucket = s.aggs.bucket('EndTime', 'date_histogram',
                              field='EndTime', interval=interval, **extra)

    curBucket = curBucket.metric('CoreHours', 'sum', field='CoreHours')
    curBucket = curBucket.metric('Records', 'sum', field='Count')

    response = s.execute()
    return response
    */

  const query = {
    size: 10000,
    query: {
      bool: {
        filter: [
          {
            range: {
              EndTime: {
                gte: startStr,
                lt: endStr,
              },
            },
          },
          {
            term: {
              ResourceType: "Batch",
            },
          },
          {
            bool: {
              must_not: [
                {
                  terms: {
                    SiteName: ["NONE", "Generic", "Obsolete"],
                  },
                },
                {
                  terms: {
                    VOName: ["Unknown", "unknown", "other"],
                  },
                },
              ],
            },
          },
        ],
      },
    },
    aggs: {
      EndTime: {
        date_histogram: {
          field: "EndTime",
          interval: "1d",
        },
        aggs: {
          CoreHours: {
            sum: {
              field: "CoreHours",
            },
          },
          Njobs: {
            sum: {
              field: "Njobs",
            },
          },
        },
      },
    },
  };

  const res = await fetch(`${endpoint}/${summaryIndex}/_search?pretty`, {
    method: "POST",
    body: JSON.stringify(query),
    headers: [["Content-Type", "application/json"]],
  });

  if (!res.ok) {
    console.error("GRACC query failed");
    console.log(await res.json());
    return null;
  }

  const body = (await res.json()) as Root;

  if (responseBodyOutputFile != null) {
    fs.writeFile(responseBodyOutputFile, JSON.stringify(body), (err) => {
      if (err) console.error("failed to write to response body output file");
    });
  }

  const hits = body.hits.hits;
  const dataPoints: JobDataPoint[] = hits.map<JobDataPoint>(
    (hit) =>
      ({
        id: hit._id,
        projectName: hit._source.ProjectName,
        cpuHours: hit._source.CoreHours,
        nJobs: hit._source.Njobs,
        endTime: hit._source.EndTime,
      } as JobDataPoint)
  );

  if (dataOutputFile != null) {
    fs.writeFile(dataOutputFile, JSON.stringify(dataPoints), (err) => {
      if (err) console.error("failed to write to data output file");
    });
  }

  return dataPoints;
}

async function main() {
  const end = isoStringToDate("2024-02-22T09:59:26+00:000");
  const start = new Date(end.getTime() - 24 * 60 * 60 * 1000);

  const result = await graccQuery(start, end);
  if (result == null) return;

  const sumJobs = result.reduce((acc, dataPoint) => acc + dataPoint.nJobs, 0);

  console.log({ sumJobs });
}

main();
