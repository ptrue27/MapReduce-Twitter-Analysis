# Analyze Twitter Data

This application uses Hadoop and MapReduce to analyze data scraped from the Twitter website. The first job `HashtagCount`finds the top hashtags included in the Tweets found in `training_set_tweets.txt`. The second job `CityCount` finds the cities with the highest number of published Tweets using the data in `training_set_tweets.txt` and `training_set_users.txt`.

## Compilation:
    The code was compiled to run on Hadoop 3.3.0 using the libraries specified in "/src/.classpath".

    The code was compiled using the Eclipse IDE with the compiler version set to 1.8.

    Export the project to a jar file, selecting the ".classpath" and ".project" files. Set the Main class to "HashtagCount" or "CityCount" for either of those respective jobs.

## Run:
    Download the files from this website: https://archive.org/details/twitter_cikm_2010, and move the txt files to HDFS. The code can be run using the following commands. Replace "~" with the desired path to the given file.

    HashtagCount: `hadoop jar ~/HashtagCount.jar ~/training_set_tweets.txt ~/HashtagCountOutput`

    CityCount: `hadoop jar ~/CityCount.jar ~/tweets.txt ~/training_set_users.txt ~/CityCountTemp ~/CityCountOutput`
