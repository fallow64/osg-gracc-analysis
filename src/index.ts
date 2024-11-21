import { Root } from "./response";
import { isoStringToDate } from "./util";
import fs from "fs";

const responseBodyOutputFile: string | null = "responseOut.json";
const dataOutputFile: string | null = "dataOut.json";

const summaryIndex = "gracc.osg.summary";
const endpoint = "https://gracc.opensciencegrid.org:443/q";

type JobDataPoint = {
  timestamp: string;
  nJobs: number;
  cpuHours: number;
};

type AnalysisResult = {
  took: number;
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
    size: 100,
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
    body: JSON.stringify(query, null, 2),
    headers: [["Content-Type", "application/json"]],
  });

  if (!res.ok) {
    console.error("GRACC query failed");
    console.log(await res.json());
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

  const totalJobs = body.hits.hits.reduce(
    (acc, hit) => acc + hit._source.CoreHours,
    0
  );

  console.log(totalJobs);

  // craft result

  const buckets = body.aggregations.EndTime.buckets;
  const result = {
    took: body.took,
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
    fs.writeFile(dataOutputFile, JSON.stringify(result, null, 2), (err) => {
      if (err) console.error("failed to write to data output file");
    });
  }

  return result;
}

async function main() {
  const end = isoStringToDate("2023-01-11T19:22:28+00:000");
  const start = new Date(end.getTime() - 1000 * 60 * 60 * 24);

  const result = await graccQuery(start, end, "1h");
  console.log(result);
}

main();
