import { Root } from "./response";
import { isoStringToDate } from "./util";
import fs from "fs";

const responseBodyOutputFile: string | null = "responseOut.json";
const dataOutputFile: string | null = "dataOut.json";

const endpoint = "https://gracc.opensciencegrid.org:443/q";
const summaryIndex = "gracc.osg.raw";

type JobDataPoint = {
  timestamp: string;
  nJobs: number;
  cpuHours: number;
};

type AnalysisResult = {
  took: number;
  startTime: string;
  endTime: string;
  sumJobs: number;
  sumCpuHours: number;
  dataPoints: JobDataPoint[];
};

async function graccQuery(
  start: string | Date,
  end: string | Date,
  interval: string,
  offset?: number
): Promise<AnalysisResult | null> {
  const startStr = typeof start === "string" ? start : start.toISOString();
  const endStr = typeof end === "string" ? end : end.toISOString();
  const offsetStr = offset != null ? `-${offset}s` : null;

  // perform query

  const query = {
    size: 0,
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
          fixed_interval: interval,
          offset: offsetStr ?? undefined,
          extended_bounds: {
            min: startStr,
            max: endStr,
          },
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
    if (res.body != null) {
      const body = await res.text();
      console.error(body);
    }
    return null;
  }

  // body response is in form of Root
  // note: if you change the query, you must remake response.d.ts
  // because the type declarations are manually generated from the response

  const body = (await res.json()) as Root;

  // write response output

  if (responseBodyOutputFile != null) {
    fs.writeFile(responseBodyOutputFile, JSON.stringify(body), (err) => {
      if (err) console.error("failed to write to response body output file");
    });
  }

  // craft result

  const buckets = body.aggregations.EndTime.buckets;
  const result = {
    took: body.took,
    startTime: startStr,
    endTime: endStr,
    sumJobs: buckets.reduce((acc, bucket) => acc + bucket.Njobs.value, 0),
    sumCpuHours: buckets.reduce(
      (acc, bucket) => acc + bucket.CoreHours.value || bucket.doc_count,
      0
    ),
    dataPoints: buckets.map((bucket) => ({
      timestamp: bucket.key_as_string,
      nJobs: bucket.Njobs.value,
      cpuHours: bucket.CoreHours.value,
    })),
  };

  // write data output

  if (dataOutputFile != null) {
    fs.writeFile(dataOutputFile, JSON.stringify(result), (err) => {
      if (err) console.error("failed to write to data output file");
    });
  }

  return result;
}

async function main() {
  const end = isoStringToDate("2024-03-11T19:22:28+00:000");
  const start = new Date(end.getTime() - 1000 * 60 * 60 * 24 * 30);

  const result = await graccQuery(start, end, "1d");
  console.log(result);
}

main();
