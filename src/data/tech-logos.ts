// Official logos for technology badges, keyed by the course `monogram`. Courses not listed keep their text
// badge (SQL and the concept courses have no logo; dbt, Delta Lake and AWS logos are not used, see
// src/assets/logos/README.md). `invertInDark` marks a single-colour black logo shown in white on dark themes.
import python from "../assets/logos/python.svg?no-inline";
import spark from "../assets/logos/spark.svg?no-inline";
import kafka from "../assets/logos/kafka.svg?no-inline";
import airflow from "../assets/logos/airflow.svg?no-inline";
import snowflake from "../assets/logos/snowflake.svg?no-inline";
import databricks from "../assets/logos/databricks.svg?no-inline";

export interface TechLogo {
  src: string;
  invertInDark?: boolean;
}

export const TECH_LOGOS: Record<string, TechLogo> = {
  Py: { src: python },
  Sp: { src: spark },
  Kf: { src: kafka, invertInDark: true },
  Af: { src: airflow },
  Sf: { src: snowflake },
  Db: { src: databricks },
};
