/*
 * tasklogger.h
 *
 *  Created on: Aug 29, 2014
 *      Author: Farshid
 */

#ifndef TASKLOGGER_H_
#define TASKLOGGER_H_

#define ull unsigned long long

#include <iostream>
#include <fstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

using namespace std;

struct TASK
{
	unsigned kind; // JOB, JOBSETUP, MAP, REDUCE, JOBCLEANUP
	ull basetime; //Time that log starts with
	ull start;
	ull stop;
	ull size;
	//string attemptID;// = "";
	ull attemptID;
	//string hostname;// = "";
	unsigned htag;
};

struct JOB
{
	//unsigned kind; // JOB, JOBSETUP, MAP, REDUCE, JOBCLEANUP
	ull basetime; //Time that log starts with
	ull start;
	ull stop;
	ull size;
	//string attemptID;// = "";
	ull jobID;
	//string hostname;// = "";
	//unsigned htag;
	unsigned SplitNum;
};

//TASK * task;
TASK ** Task; // Task array
unsigned tSize = 0; // size of the tasks

JOB ** Job; // Task array
unsigned jSize = 0; // size of the jobs

string line;

ull GetBaseTime(string line);
ull GetAttemptID(unsigned i);
ull GetJobID(unsigned i);
unsigned GetNextNum(unsigned i);

#endif /* TASKLOGGER_H_ */
