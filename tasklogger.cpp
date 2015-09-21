/*
 * tasklogger.cpp
 *
 *  Created on: Aug 29, 2014
 *      Author: Farshid
 */

#include "tasklogger.h"


ull GetBaseTime(string line)
{
	unsigned ti = 0;
	while((ti<line.length())&&(line[ti] != ':'))
		ti++;
	if ((ti<line.length())&&(line.compare(ti+1,4,"2014") == 0))
	{
		ti += 6; // reach to month
		ull month = (line[ti]-'0')*10 + (line[ti+1]-'0');
		ti += 3; // reach to day
		ull day = (line[ti]-'0')*10 + (line[ti+1]-'0');
		ti += 3;
		ull hour = (line[ti]-'0')*10 + (line[ti+1]-'0');
		ti += 3;
		ull minute = (line[ti]-'0')*10 + (line[ti+1]-'0');
		ti += 3;
		ull second = (line[ti]-'0')*10 + (line[ti+1]-'0');
		ti += 3;
		ull milisec = (line[ti]-'0')*100 + (line[ti+1]-'0')*10 + (line[ti+2]-'0');
		ull mstime = milisec + 1000 * (second + 60 * (minute + 60 * (hour + 24 * ((day-1) + 30 * (month-1)))));
		return mstime;
	}
	else
		return -1;
}

ull GetAttemptID(unsigned i)//,j)
{
	ull r1 = (line[i]-'0')*100000 + (line[i+1]-'0')*10000 + (line[i+2]-'0')*1000 + (line[i+3]-'0')*100
			+ (line[i+4]-'0')*10 + (line[i+5]-'0'); // first, 6-digit
	ull r2 = (line[i+7]-'0')*10000 + (line[i+8]-'0')*1000 + (line[i+9]-'0')*100 + (line[i+10]-'0')*10
			+ (line[i+11]-'0'); // second, 5-digit
	ull r3 = (line[i+15]-'0')*100000 + (line[i+16]-'0')*10000 + (line[i+17]-'0')*1000 + (line[i+18]-'0')*100
			+ (line[i+19]-'0')*10 + (line[i+20]-'0'); // third, 6-digit
	return (line[22]-'0') + 10 * (r3 + 1000000 * (r2 + 100000 * r1));
}

ull GetJobID(unsigned i)
{
	ull r1 = (line[i]-'0')*100000 + (line[i+1]-'0')*10000 + (line[i+2]-'0')*1000 + (line[i+3]-'0')*100
			+ (line[i+4]-'0')*10 + (line[i+5]-'0'); // first, 6-digit
	ull r2 = (line[i+7]-'0')*10000 + (line[i+8]-'0')*1000 + (line[i+9]-'0')*100 + (line[i+10]-'0')*10
			+ (line[i+11]-'0'); // second, 5-digit
	return r2 + r1 * 100000;
}

unsigned GetNextNum(unsigned i)
{
	while ((i < line.length()) && (( ((int)(line[i])) >= (int)'0' ) && ( ((int)(line[i])) <= (int)'9' )) )
	// between 0 - 9
		i++;
	unsigned r = 0;
	while ((i < line.length()) && (( ((int)(line[i])) >= (int)'0' ) && ( ((int)(line[i])) <= (int)'9' )) )
	{
		r =  r * 10 + (line[i]-'0');
		i++;
	}
	if(i>=line.length())
		exit(EXIT_FAILURE);
	return r;
}

//void AddTask()
//{
//}

// hadoop-npi_e-jobtracker-wdscnl094.log.2014-04-14:2014-04-14 00:54:20,824 ...
// INFO org.apache.hadoop.mapred.JobInProgress: ...
// Input size for job job_201404012233_57242 = 3100. Number of splits = 4

int main ( int arc, char **argv )
{//StatCol filename
	/////////////////////////Just4debug

	string logpath =  "Debug/hadoop-npi_e-jobtracker-wdscnl094.log.2014-04-14";//argv[1];
	ifstream logfile (&logpath[0]); // command line program gets log filename

	string tplotpath = logpath + ".t.dat"; // plot file name
	ofstream tplotfile (&tplotpath[0]);
	string jplotpath = logpath + ".j.dat"; // plot file name
	ofstream jplotfile (&jplotpath[0]);

	//fseek (logfile, 0, SEEK_END);   // non-portable
	//ftell (pFile) / 1000;
	TASK ** Task = new TASK* [10000000]; // it is impossible more than 10M jobs!!!
	JOB ** Job  = new JOB*  [100000]; // it is impossible more than 100K jobs!!!

	/////////////////////////Link Analysis
	if (tplotfile.is_open() && jplotfile.is_open())
	{
		if (logfile.is_open())
		{
			if(!getline (logfile,line))
				return 1; // no line inside the log!!!
			ull BaseTime = GetBaseTime(line);
			do
			{// Now log is ready to analyze!
				unsigned ti = 0;//timestamp index
				//while((ti<line.length())&&(line[ti] != ':'))
				//	ti++;
				if ((ti<line.length())&&(line.compare(ti,4,"2014") == 0))
				{
					ti += 6; // reach to month
					ull month = (line[ti]-'0')*10 + (line[ti+1]-'0');
					ti += 3; // reach to day
					ull day = (line[ti]-'0')*10 + (line[ti+1]-'0');
					ti += 3;
					ull hour = (line[ti]-'0')*10 + (line[ti+1]-'0');
					ti += 3;
					ull minute = (line[ti]-'0')*10 + (line[ti+1]-'0');
					ti += 3;
					ull second = (line[ti]-'0')*10 + (line[ti+1]-'0');
					ti += 3;
					ull milisec = (line[ti]-'0')*100 + (line[ti+1]-'0')*10 + (line[ti+2]-'0');
					ull logtime = milisec + 1000 * (second + 60 * (minute + 60 * (hour + 24 * ((day-1) + 30 * (month-1)))));
					ti = 55; // Pass some characters as a default
					while((ti<line.length())&&(line[ti] != ':'))
						ti++; // Reach to next colon ":"
					ti += 2; // Reach to first keyword
					if     ((ti<line.length())&&(line.compare(ti,11,"Adding task") == 0))
					{//Adding new task
						TASK* new_task = new TASK;
						Task[tSize] = new_task;
						tSize++;
						new_task->start = logtime;
						new_task->basetime = BaseTime;
						new_task->kind = line[ti+13]; // S M R C
						while((ti<line.length())&&(line[ti] != '\'')) // Adding task (MAP) 'attempt_3224...
							ti++; // Reach to next colon ":"
						ti += 15;
						if(ti>=line.length())
							exit(EXIT_FAILURE);
						unsigned ti2 = ti + 1;
						while((ti2<line.length())&&(line[ti2] != '\'')) // Adding task (MAP) 'attempt_3224...
							ti2++; // Reach to next colon " ' "
						if(ti2>=line.length())
							exit(EXIT_FAILURE);
						new_task->attemptID = GetAttemptID(ti);//,ti2);
						ti += ti2 + 2;
						while((ti<line.length())&&(line[ti] != '\'')) // Adding task (MAP) 'attempt_3224...
							ti++; // Reach to next colon " ' "
						if(ti>=line.length())
							exit(EXIT_FAILURE);
						new_task->htag = GetNextNum(ti + 1);
						//new_task->stop = ?
						//new_task->size = ?
					}
					else if((ti<line.length())&&(line.compare(ti,4,"Task") == 0))
					{//Task completion
						//Find the corresponding task in the array by o(log n)
						//set stop time
						ti += 20; // Reach to attempt id
						if(ti>=line.length())
							exit(EXIT_FAILURE);
						ull attemptID = GetAttemptID(ti);
						long bi = tSize - 1;
						while ((bi>=0) && (Task[bi]->attemptID != attemptID))
							bi--;
						if(bi>=0)
							Task[bi]->stop = logtime;
					}
					else if((ti<line.length())&&(line.compare(ti,3,"Job") == 0))
					{//Job submission or completion
						if(line[ti+31] == 'c')//completion
						{
							//Find the completed job
							ti += 14; // Reach to job id
							if(ti>=line.length())
								exit(EXIT_FAILURE);
							ull jobID = GetJobID(ti);
							long bi = tSize - 1;
							while ((bi>=0) && (Job[bi]->jobID != jobID))
								bi--;
							if(bi>=0)
								Job[bi]->stop = logtime;
						}
						else
						{
							//
						}
					}
					else if((ti<line.length())&&(line.compare(ti,10,"Input size") == 0))
					{// Job submission
						JOB* new_job = new JOB;
						Job[jSize] = new_job;
						jSize++;
						new_job->start = logtime;
						new_job->basetime = BaseTime;
						//new_job->kind = line[ti+13]; // S M R C
						ti += 29; // Reach to job ID
						if(ti>=line.length())
							exit(EXIT_FAILURE);
						new_job->jobID = GetJobID(ti);
						ti += 15;
						if(ti>=line.length())
							exit(EXIT_FAILURE);
						new_job->size = GetNextNum(ti);
						while((ti<line.length())&&(line[ti] != '=')) // Adding task (MAP) 'attempt_3224...
							ti++; // Reach to next colon ":"
						if(ti>=line.length())
							exit(EXIT_FAILURE);
						new_job->SplitNum = GetNextNum(ti);
					}
				}
			}
			while (getline (logfile,line));
			logfile.close();
		}
		//tplotfile << splitsize << "\n";
		//cout << splitsize << "\n";
		tplotfile.close();
		jplotfile.close();
	}
	return 0; // Indicates that everything went well.
}






