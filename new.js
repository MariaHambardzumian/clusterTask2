////////////////==================================WIthout cluster
import fs from 'fs'
import csv from 'csv-parser'
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


for (let i = 0; i < files.length; i++) {
    await parseData(i)

}


function parseData(num) {
    const results = [];

    return new Promise((resolve, reject) => {

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

                fs.writeFile(path.join(outputFile, fileName), jsonData, (err) => {
                    if (err) {
                        reject('Error writing JSON file:' + err)
                    } else {       
                        resolve(results)
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
})
