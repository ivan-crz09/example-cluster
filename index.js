require("dotenv").config(".env");
(function () {
  main();
})();

function main() {
  const numCPUs = require("os").cpus().length;
  const cluster = require("cluster");
  const cpu = numCPUs;
  if (cluster.isMaster) {
    masterProcess(cluster, cpu);
  } else {
    childProcess(cluster, cpu);
  }
}

function masterProcess(cluster, cpu) {
  console.log(`Master ${process.pid} is running`);

  // Fork workers
  const workers = [];
  for (let i = 0; i < cpu; i++) {
    console.log(`Forking process number ${i}...`);

    const worker = cluster.fork();
    workers.push(worker);

    // Listen for messages from worker
    worker.on("message", function (message) {
      console.log(
        `Master ${process.pid} recevies message '${JSON.stringify(
          message
        )}' from worker ${worker.process.pid}`
      );
    });
  }

  // Send message to the workers
  let started = 0;
  workers.forEach(function (worker) {
    let total = process.env.TOTAL_ARTICLES; // total articles
    total = total / cpu;
    console.log(
      `Master ${process.pid} sends message to worker ${worker.process.pid}...`
    );
    worker.send({
      articles: total,
      started: started,
      msg: `Message from master ${process.pid},started:${started}, send articles: ${total}`,
    });
    started = started + total;
  }, this);
}

function childProcess(cluster) {
  var start = new Date();
  process.on("message", function (message) {
    var MongoClient = require("mongodb").MongoClient;
    const uri = `mongodb://${process.env.DB_MONGO_USERNAME}:${process.env.DB_MONGO_PASSWORD}@${process.env.DB_MONGO_HOST}:${process.env.DB_MONGO_PORT}/${process.env.DB_MONGO_DB}?${process.env.DB_MONGO_OPTIONS}`;
    MongoClient.connect(uri, function (err, client) {
      if (err) {
        console.log(err);
        throw err;
      }
      var db = client.db(process.env.DB_MONGO_DB);
      db.collection(process.env.API_COLLECTION)
        .find({})
        .skip(Number(message.started))
        .limit(Number(message.articles))
        .toArray()
        .then((data, error) => {
          console.log("error: ", error);
          console.log("total articles by cluster: ", data.length);
          var end = new Date() - start;
          console.info("Execution time: %dms", end);
          client.close();
        })
        .catch(console.log);

      console.log(
        `Worker ${process.pid} recevies message '${JSON.stringify(message)}'`
      );
    });
  });
  console.log(`Worker ${process.pid} finished`);
}
