# Sparkifydb

The sparkify database is utilize to store and analyze data about songs and users colleted by the Sparkify streaming app.

## Sparkifydb Database Schema
The shcema of the database was created based on the song and log and datasets. The song dataset is in JSON format and contains metadata about the song and the artist of that song. The log dataset is also in JSON format and contains activity logs from the Sparkify streaming app.

In order to support and optimize the queries for the song play analysis, an star shcema model was implemented whith the following tables:


![Images](./sparkifydb.png)

<ul>

**<li>Fact Table</li>**
<ul>

*<li>Songplays</li>*
<table id="public.songplays" class="table">
	<caption class="tab-name">
		<em>public</em>.<strong>songplays</strong>
		<span class="type-label">Table</span>
	</caption>
	<thead>
		<tr>
			<th>Name</th>
			<th>Data type</th>
			<th>PK</th>
			<th>FK</th>
			<th>UQ</th>
			<th>Not null</th>
			<th>Default value</th>
			<th>Description</th>
		</tr>
	</thead>
	<tbody>
	<tr>
		<td>songplay_id</td>
		<td class="data-type">integer</td>
		<td class="bool-field">&#10003;</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field">&#10003;</td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>start_time</td>
		<td class="data-type">bigint</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field">&#10003;</td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>user_id</td>
		<td class="data-type">integer</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field">&#10003;</td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>level</td>
		<td class="data-type">character varying</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>song_id</td>
		<td class="data-type">character varying</td>
		<td class="bool-field"></td>
		<td class="bool-field">&#10003;</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>artist_id</td>
		<td class="data-type">character varying</td>
		<td class="bool-field"></td>
		<td class="bool-field">&#10003;</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>session_id</td>
		<td class="data-type">character varying</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>location</td>
		<td class="data-type">character varying</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>user_agent</td>
		<td class="data-type">character varying</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td colspan="8" class="nested-tab-parent">
			<table class="nested-tab">
				<tr>
					<td class="title" colspan="5">Constraints</td>
				</tr>
				<tr>
					<td class="title">Name</td>
					<td class="title">Type</td>
					<td class="title">Column(s)</td>
					<td class="title">References</td>
					<td class="title">Description</td>
				</tr>
				<tr>
					<td>songplays_pkey</td>
					<td class="value constr-type">PRIMARY KEY</td>
					<td>songplay_id</td>
					<td></td>
					<td colspan="4"><em></em></td>
				</tr>
			</table>
		</td>
	</tr>
	</tbody>
</table>

</ul>

**<li>Dimension Tables</li>**
<ul>

*<li>Users</li>*
<table id="public.users" class="table">
	<caption class="tab-name">
		<em>public</em>.<strong>users</strong>
		<span class="type-label">Table</span>
	</caption>
	<thead>
		<tr>
			<th>Name</th>
			<th>Data type</th>
			<th>PK</th>
			<th>FK</th>
			<th>UQ</th>
			<th>Not null</th>
			<th>Default value</th>
			<th>Description</th>
		</tr>
	</thead>
	<tbody>
	<tr>
		<td>user_id</td>
		<td class="data-type">integer</td>
		<td class="bool-field">&#10003;</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field">&#10003;</td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>first_name</td>
		<td class="data-type">character varying</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>last_name</td>
		<td class="data-type">character varying</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>gender</td>
		<td class="data-type">character varying</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>level</td>
		<td class="data-type">character varying</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	</tbody>
</table>

*<li>Songs</li>*
<table id="public.songs" class="table">
	<caption class="tab-name">
		<em>public</em>.<strong>songs</strong>
		<span class="type-label">Table</span>
	</caption>
	<thead>
		<tr>
			<th>Name</th>
			<th>Data type</th>
			<th>PK</th>
			<th>FK</th>
			<th>UQ</th>
			<th>Not null</th>
			<th>Default value</th>
			<th>Description</th>
		</tr>
	</thead>
	<tbody>
	<tr>
		<td>song_id</td>
		<td class="data-type">character varying</td>
		<td class="bool-field">&#10003;</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field">&#10003;</td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>title</td>
		<td class="data-type">character varying</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>artist_id</td>
		<td class="data-type">character varying</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>year</td>
		<td class="data-type">integer</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>duration</td>
		<td class="data-type">numeric</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	</tbody>
</table>

*<li>Artists</li>*
<table id="public.artists" class="table">
	<caption class="tab-name">
		<em>public</em>.<strong>artists</strong>
		<span class="type-label">Table</span>
	</caption>
	<thead>
		<tr>
			<th>Name</th>
			<th>Data type</th>
			<th>PK</th>
			<th>FK</th>
			<th>UQ</th>
			<th>Not null</th>
			<th>Default value</th>
			<th>Description</th>
		</tr>
	</thead>
	<tbody>
	<tr>
		<td>artist_id</td>
		<td class="data-type">character varying</td>
		<td class="bool-field">&#10003;</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field">&#10003;</td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>name</td>
		<td class="data-type">character varying</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>location</td>
		<td class="data-type">character varying</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>latitude</td>
		<td class="data-type">numeric</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>longitude</td>
		<td class="data-type">numeric</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	</tbody>
</table>

*<li>Time</li>*
<table id="public.time" class="table">
	<caption class="tab-name">
		<em>public</em>.<strong>time</strong>
		<span class="type-label">Table</span>
	</caption>
	<thead>
		<tr>
			<th>Name</th>
			<th>Data type</th>
			<th>PK</th>
			<th>FK</th>
			<th>UQ</th>
			<th>Not null</th>
			<th>Default value</th>
			<th>Description</th>
		</tr>
	</thead>
	<tbody>
	<tr>
		<td>time_id</td>
		<td class="data-type">integer</td>
		<td class="bool-field">&#10003;</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field">&#10003;</td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>star_time</td>
		<td class="data-type">time</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>hour</td>
		<td class="data-type">integer</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>day</td>
		<td class="data-type">integer</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>week</td>
		<td class="data-type">integer</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>month</td>
		<td class="data-type">integer</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>year</td>
		<td class="data-type">integer</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	<tr>
		<td>weekday</td>
		<td class="data-type">integer</td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="bool-field"></td>
		<td class="value"></td>
		<td><em></em></td>
	</tr>
	</tbody>
</table>

</ul>
</ul>


## How to Setup Sparkifydb
<ol>

**<li>Run create_tables.py</li>**
This Python script creates the Sparkifydb and all the tables above.

**<li>Run etl.py</li>**
This is the ETL pipeline used to load the data from the JSON files into the Sparkifydb database.

<li>Run the test.ipynb notebook to query the database and make sure the data was loaded correctly.</li>
</ol>

