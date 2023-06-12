import fs from 'fs'
import csv from 'csv-parser'
import cluster from 'cluster'
import path from 'path'


let dirname = process.argv[2]
const records = {
    start: Date.now(),
}
const files = fs.readdirSync(dirname).filter((file) => {
    return path.extname(file) == '.csv'
});

if (!files.length) {
    throw new Error('no such file(s)')
}

if (cluster.isPrimary) {
    // console.log(`Primary ${process.pid} is running`);

    for (let i = 0; i < files.length; i++) {
        cluster.fork();
    }

    cluster.on('message',  (worker, message) => {
        if(message.done){
            // console.log(`message from  worker ${worker.id}:`);
            // console.log('JSON file has been written successfully! '); 
            records[message.filename] = message.duration_amount
        }
      });
} else {
    
    parseData(cluster.worker.id-1)

}

function parseData(num) {
    const results = [];

    return new Promise((resolve, reject)=>{     
        
        fs.createReadStream(files[num])
        .pipe(csv())
        .on('data', (data) => {
            results.push(data)
        }
        )
        .on('end', () => {
                const jsonData = JSON.stringify(results);

                let outputFile = path.resolve(dirname, 'converted')

                let fileName = `${path.basename(files[num], '.csv')}.json`

                 if (!fs.existsSync(outputFile)) {
                    fs.mkdirSync(outputFile);
                }

                fs.writeFile(path.join(outputFile, fileName) , jsonData, (err) => {
                  if (err) {
                    reject('Error writing JSON file:' + err) 
                  } else {
                    process.send({
                        done:true, 
                        filename: fileName, 
                        duration_amount : [Date.now() - records.start, results.length]
                    })
                    resolve(results) 
                    process.kill(process.pid)
                  }
                });
        })
        .on('error', (error) => {
            reject('Error writing JSON file:' + error)
        });
    })     
}


function formatDateTime(timestamp) {
    const currentDate = new Date(timestamp);
  
    const padZero = (value) => value.toString().padStart(2, '0');
    
    const year = currentDate.getFullYear();
    const month = padZero(currentDate.getMonth() + 1);
    const day = padZero(currentDate.getDate());
    const hour = padZero(currentDate.getHours());
    const minutes = padZero(currentDate.getMinutes());
    const seconds = padZero(currentDate.getSeconds());
    const milliseconds = padZero(currentDate.getMilliseconds()).padStart(3, '0');
  
    return `${year}/${month}/${day}_${hour}:${minutes}:${seconds}:${milliseconds}`;
  }


process.on('exit', () => {
    console.log(`Process started ${formatDateTime(records.start)}`);
    console.log(`Process cost ${Date.now() - records.start} miliseconds`);
    delete records.start
    let overall = 0
    for (const key in records) {
       let [duration, amount] = records[key]
       console.log(`${key} took ${duration} miliseconds to parse, ${amount} amount of data has been read/written`)
       overall += amount
    }
    console.log(`overall ${overall} amount of data has been read/written`);
})
