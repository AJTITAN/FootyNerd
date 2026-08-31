require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SECRET_KEY || process.env.SUPABASE_PUBLISHABLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error('❌ Error: Missing Supabase credentials in .env file.');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey, {
  auth: { persistSession: false, autoRefreshToken: false },
});

const dataFilePath = path.join(__dirname, 'data', 'sample_match.json');
console.log(` Loading JSON data from: ${dataFilePath}`);

if (!fs.existsSync(dataFilePath)) {
  console.error(`❌ Data file not found at ${dataFilePath}`);
  process.exit(1);
}

const rawData = JSON.parse(fs.readFileSync(dataFilePath, 'utf-8'));
console.log(` Loaded ${rawData.length} events from match data.`);

const matchId = 3869685; 
const startingXIEvents = rawData.filter((e) => e.type?.name === 'Starting XI');
const homeTeamName = startingXIEvents[0]?.team?.name || 'Argentina';
const awayTeamName = startingXIEvents[1]?.team?.name || 'France';
let homeScore = 0;
let awayScore = 0;

rawData.forEach((event) => {
  if (event.type?.name === 'Shot' && event.shot?.outcome?.name === 'Goal' && event.period <= 4) {
    if (event.team?.name === homeTeamName) {
      homeScore++;
    } else if (event.team?.name === awayTeamName) {
      awayScore++;
    }
  }
});

const matchRecord = {
  id: String(matchId),
  match_id: matchId,
  home_team: homeTeamName,
  away_team: awayTeamName,
  home_score: homeScore,
  away_score: awayScore,
  competition_name: 'FIFA World Cup 2022 - Final',
  match_date: '2022-12-18',
};

function transformEvent(event) {
  const location_x = event.location ? Number(event.location[0]) : null;
  const location_y = event.location ? Number(event.location[1]) : null;

  let end_location_x = null;
  let end_location_y = null;
  let outcome = null;
  let recipient_name = null;
  let xg = null;

  if (event.type?.name === 'Pass' && event.pass) {
    end_location_x = event.pass.end_location ? Number(event.pass.end_location[0]) : null;
    end_location_y = event.pass.end_location ? Number(event.pass.end_location[1]) : null;
    outcome = event.pass.outcome ? event.pass.outcome.name : 'Complete';
    recipient_name = event.pass.recipient ? event.pass.recipient.name : null;
  } else if (event.type?.name === 'Shot' && event.shot) {
    end_location_x = event.shot.end_location ? Number(event.shot.end_location[0]) : null;
    end_location_y = event.shot.end_location ? Number(event.shot.end_location[1]) : null;
    outcome = event.shot.outcome ? event.shot.outcome.name : null;
    xg = event.shot.statsbomb_xg ? Number(event.shot.statsbomb_xg) : null;
  } else if (event.type?.name === 'Carry' && event.carry) {
    end_location_x = event.carry.end_location ? Number(event.carry.end_location[0]) : null;
    end_location_y = event.carry.end_location ? Number(event.carry.end_location[1]) : null;
  } else if (event.dribble) {
    outcome = event.dribble.outcome ? event.dribble.outcome.name : null;
  } else if (event.duel) {
    outcome = event.duel.outcome ? event.duel.outcome.name : null;
  } else if (event.interception) {
    outcome = event.interception.outcome ? event.interception.outcome.name : null;
  }

  return {
    id: event.id,
    match_id: matchId,
    event_index: event.index,
    period: event.period,
    minute: event.minute,
    second: event.second,
    timestamp: event.timestamp || null,
    team_name: event.team?.name || 'Unknown',
    player_id: event.player?.id || null,
    player_name: event.player?.name || null,
    type_name: event.type?.name || 'Unknown',
    play_pattern: event.play_pattern?.name || null,
    under_pressure: Boolean(event.under_pressure),
    duration: event.duration !== undefined ? Number(event.duration) : null,
    location_x,
    location_y,
    end_location_x,
    end_location_y,
    outcome,
    xg,
    recipient_name,
  };
}

function chunkArray(array, size) {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

async function runPipeline() {
  console.log('\n--- 1. UPSERTING MATCH HEADER ---');
  console.log(matchRecord);

  const { error: matchError } = await supabase
    .from('matches')
    .upsert(matchRecord, { onConflict: 'match_id' });

  if (matchError) {
    console.error('Failed to insert match record:', matchError.message);
    return;
  }
  console.log(' Match header inserted successfully!');

  console.log('\n--- 2. TRANSFORMING ALL EVENTS ---');
  const transformedEvents = rawData.map(transformEvent);
  console.log(`Transformed ${transformedEvents.length} events.`);

  console.log('\n--- 3. BATCH INSERTING EVENTS ---');
  const BATCH_SIZE = 500;
  const batches = chunkArray(transformedEvents, BATCH_SIZE);
  console.log(`Split into ${batches.length} batches of up to ${BATCH_SIZE} events each.`);

  let totalInserted = 0;
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    const { error: batchError } = await supabase
      .from('events')
      .upsert(batch, { onConflict: 'id' });

    if (batchError) {
      console.error(`Batch ${i + 1}/${batches.length} failed:`, batchError.message);
    } else {
      totalInserted += batch.length;
      console.log(`[Batch ${i + 1}/${batches.length}] Inserted ${totalInserted} / ${transformedEvents.length} events`);
    }
  }

  if (totalInserted === transformedEvents.length) {
    console.log(`\nALL ${totalInserted} EVENTS INGESTED SUCCESSFULLY!`);
  } else {
    console.log(`\nFinished with partial ingestion: ${totalInserted} / ${transformedEvents.length} events inserted.`);
  }
}

runPipeline();