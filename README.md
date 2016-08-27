# WeblogChallenge
Weblog challenge - parsing weblogs with big data tools


###Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

2. Determine the average session time

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4. Find the most engaged users, ie the IPs with the longest session times


###Tools used:
- Spark 2.0 via pyspark
- Pig 0.16 with the datafu 1.2.0 library

###How to use and run the code
- The code was developed and tested on OSX 10.9 with up to date Homebrew. Spark and Pig were installed with "brew install apache-spark pig" and both run in local mode with JDK 1.8.0_102
- Besides the full dataset in `data/`, there is a smaller sample dataset in `sample.log` for quick tests during the interactive development. 
- The large dataset needs to be manually unpacked in a file called `full.log` in the same directory where the scripts are located:

		git clone git@github.com:marikgoran/WeblogChallenge.git
		cd WeblogChallenge
		gunzip -c data/2015_07_22_mktplace_shop_web_log_sample.log.gz > full.log

- The pyspark code is in python3, so it needs to be have `PYSPARK_PYTHON=python3` in the environment: `PYSPARK_PYTHON=python3 spark-submit sessionize.spark.py`. The latest python3.5.2 from homebrew was used in the development, but 3.2+ should work as well.
- The Pig code needs to source the datafu.jar, which is included in the repo. It can be run with `pig -x local sessionize.pig`
- Both scripts will by default output the answer of the question 4 from the [Processing & Analytical goals] section above.
- Unfortunately the scripts are not fully automated at the moment, so some commenting and un-commenting in the code are needed in order to get the output for the other questions.
- The Pig script **does not** give a correct answer on question 3 at the moment - that code is in development and should be ready soon.

--
###Additional notes and comments:
- IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions
