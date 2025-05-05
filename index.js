import inquirer from "inquirer";
import { Client } from "pg";

const client = new Client({
  user: 'roblox_game_data_postgres_user',
  host: 'dpg-d0aimder433s73foamrg-a.oregon-postgres.render.com',
  database: 'roblox_game_data_postgres',
  password: 'VJETySm3LWHF4nuUrJXp4kZnPVwumLQ6',
  port: 5432,
  ssl: {
    rejectUnauthorized: false  // accept self-signed certs (Render's default)
  }
});

const getGame = async () => {
    const {game_name} = await inquirer.prompt([
        {
            type: 'input',
            name: 'game_name',
            message: 'Enter the name of the game you want to search for:'
        }
    ]);
    const res = await client.query('SELECT * FROM game_stats WHERE game_name = $1;', [game_name]);
    if (res.rowCount === 0) {
        return undefined
    } else {
        return game_name
    }
}

const getRow = async (game_name) => {
    const {date} = await inquirer.prompt([
        {
            type: 'input',
            name: 'date',
            message: 'Enter the date you want to search for (YYYY-MM-DD):'
        }
    ])
    const res = await client.query('SELECT * FROM game_stats WHERE game_name = $1 AND date = $2;', [game_name, date]);
    if (res.rowCount === 0) {
        console.log('No data found for the specified date.');
    } else {
        console.log(`Data for ${game_name} on ${date}:`);
        console.table(res.rows);
    }
}

const analyzeStat = async (game_name) => {
    const {stat} = await inquirer.prompt([
        {
            type: 'input',
            name: 'stat',
            message: 'Enter the stat you want to analyze:'
        }
    ])
    const res = await client.query('SELECT $2 FROM game_stats WHERE game_name = $1;', [game_name, stat]);
    if (res.rowCount === 0) {
        return undefined
    } else {
        return stat
    }
}

const allowedColumns = [
  'dau', 'avg_playtime', 'd1_ret', 'd1_stick', 'd7_ret', 'd7_stick',
  'qptr_user', 'arppu', 'pcr', 'impressions'
];

const getNHighestValues = async (game_name, stat) => {
  if (!allowedColumns.includes(stat)) {
      console.log('Invalid stat provided.');
      return;
  }

  const { n } = await inquirer.prompt([
      {
          type: 'input',
          name: 'n',
          message: 'Enter the number of highest values you want to find:'
      }
  ]);

  const query = `
      SELECT * FROM game_stats
      WHERE game_name = $1
      ORDER BY ${stat} DESC
      LIMIT $2;
  `;

  const res = await client.query(query, [game_name, n]);

  if (res.rowCount === 0) {
      console.log('No data found for the specified stat.');
  } else {
      console.log(`Top ${n} highest values for ${stat} in ${game_name}:`);
      console.table(res.rows.reverse()); // optional: reverse to show ascending order
  }
};

const getNLowestValues = async (game_name, stat) => {
  if (!allowedColumns.includes(stat)) {
      console.log('Invalid stat provided.');
      return;
  }

  const { n } = await inquirer.prompt([
      {
          type: 'input',
          name: 'n',
          message: 'Enter the number of lowest values you want to find:'
      }
  ]);

  const query = `
      SELECT * FROM game_stats
      WHERE game_name = $1
      ORDER BY ${stat} ASC
      LIMIT $2;
  `;

  const res = await client.query(query, [game_name, n]);

  if (res.rowCount === 0) {
      console.log('No data found for the specified stat.');
  } else {
      console.log(`Top ${n} lowest values for ${stat} in ${game_name}:`);
      console.table(res.rows);
  }
};


const getAvg = async (game_name) => {
    const {x} = await inquirer.prompt([
        {
            type: 'input',
            name: 'x',
            message: 'Enter the number of prior day stats to average:'
        }
    ])
    const {y} = await inquirer.prompt([
        {
            type: 'input',
            name: 'y',
            message: 'Enter the number of top impressions to find:'
        }
    ])
    const res = await client.query(`
        WITH top_days AS (
  SELECT date
  FROM game_stats
  WHERE game_name = $1
  ORDER BY impressions DESC
  LIMIT $2
)
SELECT
  td.date AS top_day,
  gs.impressions,
  ROUND((
    SELECT AVG(dau)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < td.date AND date >= td.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_dau,
  ROUND((
    SELECT AVG(avg_playtime)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < td.date AND date >= td.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_playtime,
  ROUND((
    SELECT AVG(d1_ret)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < td.date AND date >= td.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_d1_ret,
  ROUND((
    SELECT AVG(d1_stick)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < td.date AND date >= td.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_d1_stick,
  ROUND((
    SELECT AVG(d7_ret)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < td.date AND date >= td.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_d7_ret,
  ROUND((
    SELECT AVG(d7_stick)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < td.date AND date >= td.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_d7_stick,
  ROUND((
    SELECT AVG(qptr_user)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < td.date AND date >= td.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_qptr_user,
  ROUND((
    SELECT AVG(qptr_rec)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < td.date AND date >= td.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_qptr_rec,
  ROUND((
    SELECT AVG(arppu)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < td.date AND date >= td.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_arppu,
  ROUND((
    SELECT AVG(pcr)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < td.date AND date >= td.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_pcr,
  ROUND((
    SELECT AVG(impressions)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < td.date AND date >= td.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_impressions
FROM top_days td
JOIN game_stats gs ON gs.date = td.date AND gs.game_name = $1
ORDER BY gs.impressions DESC;

`, [game_name, y, x]);
    if (res.rowCount === 0) {
        console.log('No data found for the specified game.');
    } else {
        console.log(`Top ${x} days with the highest average impressions:`);
        console.table(res.rows);
    }
}

const getLowestAvg = async (game_name) => {
    const {x} = await inquirer.prompt([
        {
            type: 'input',
            name: 'x',
            message: 'Enter the number of prior day stats to average:'
        }
    ])
    const {y} = await inquirer.prompt([
        {
            type: 'input',
            name: 'y',
            message: 'Enter the number of top impressions to find:'
        }
    ])

    const res = await client.query(`
WITH bottom_days AS (
  SELECT date
  FROM game_stats
  WHERE game_name = $1
  ORDER BY impressions ASC
  LIMIT $2
)
SELECT
  bd.date AS low_day,
  gs.impressions,
  ROUND((
    SELECT AVG(dau)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < bd.date AND date >= bd.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_dau,
  ROUND((
    SELECT AVG(avg_playtime)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < bd.date AND date >= bd.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_playtime,
  ROUND((
    SELECT AVG(d1_ret)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < bd.date AND date >= bd.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_d1_ret,
  ROUND((
    SELECT AVG(d1_stick)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < bd.date AND date >= bd.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_d1_stick,
  ROUND((
    SELECT AVG(d7_ret)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < bd.date AND date >= bd.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_d7_ret,
  ROUND((
    SELECT AVG(d7_stick)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < bd.date AND date >= bd.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_d7_stick,
  ROUND((
    SELECT AVG(qptr_user)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < bd.date AND date >= bd.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_qptr_user,
  ROUND((
    SELECT AVG(qptr_rec)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < bd.date AND date >= bd.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_qptr_rec,
  ROUND((
    SELECT AVG(arppu)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < bd.date AND date >= bd.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_arppu,
  ROUND((
    SELECT AVG(pcr)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < bd.date AND date >= bd.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_pcr,
  ROUND((
    SELECT AVG(impressions)::numeric
    FROM game_stats
    WHERE game_name = $1 AND date < bd.date AND date >= bd.date - ($3 * INTERVAL '1 day')
  ), 3) AS avg_impressions
FROM bottom_days bd
JOIN game_stats gs ON gs.date = bd.date AND gs.game_name = $1
ORDER BY gs.impressions ASC;
`, [game_name, y, x]);
    if (res.rowCount === 0) {
        console.log('No data found for the specified game.');   
    } else {
        console.log(`Top ${x} days with the lowest average impressions:`);
        console.table(res.rows);
    }
    return res.rows;
}



const main = async () => {
  try {
    await client.connect();
    console.log('Connected to the database!'); 
  } catch (err) {
    console.error('Error connecting to the database:', err);
    return;
  } 

  let count = 0;
  let game_name = undefined;
  while (game_name === undefined) {
    game_name = await getGame();
    if (game_name === undefined) {
        console.log('Game not found. Please try again.');
        count++;
        if (count >= 3) {
            console.log('Too many attempts. Exiting...');
            break;
        }
    } else {
        console.log(`Game found: ${game_name}`);
    }
  }

  const {action} = await inquirer.prompt([
    {
        type: 'list',
        name: 'action',
        message: 'What do you want to do?',
        choices: ['Get data for a specific date', 'Analyze a stat', 'Find avg of prior x-day stats to top y impressions', 'Find avg of prior x-day stats to bottom y impressions', 'Exit']
    }
  ]);

  if (action === 'Get data for a specific date') { 
    await getRow(game_name);
  } else if (action === 'Analyze a stat') { 
    let count = 0;
    let stat = undefined;
    while (stat === undefined) {
        stat = await analyzeStat(game_name);
        if (stat === undefined) {
            console.log('Stat not found. Please try again.');
            count++;
            if (count >= 3) {
                console.log('Too many attempts. Exiting...');
                break;
            }
        } else {
            console.log(`Stat found: ${stat}`);
        }
    }
    const {action2} = await inquirer.prompt([
        {
            type: 'list',
            name: 'action2',
            message: 'What do you want to do?',
            choices: ['Find n highest values', 'Find n lowest values', 'Exit']
        }
    ]);

    if (action2 === 'Find n highest values') {
        await getNHighestValues(game_name, stat);
    } else if (action2 === 'Find n lowest values') {
        await getNLowestValues(game_name, stat);
    } else {
        console.log('Exiting...');
        return; 
    }
  } else if (action === 'Find avg of prior x-day stats to top y impressions') {
    await getAvg(game_name);
  } else if (action === 'Find avg of prior x-day stats to bottom y impressions') {
    await getLowestAvg(game_name);
  } else if (action === 'Exit') {
    console.log('Exiting...');
    return; 
  } 

  await client.end();
};

main();
