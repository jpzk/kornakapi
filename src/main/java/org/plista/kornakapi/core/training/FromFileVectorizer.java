package org.plista.kornakapi.core.training;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.text.SequenceFilesFromDirectory;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;
import org.plista.kornakapi.core.config.LDARecommenderConfig;

import com.google.common.collect.Lists;

/**
 * 
 *
 */
public class FromFileVectorizer {


	private Path DocumentFilesPath;
	private Path sequenceFilesPath;
	private Path sparseVectorOut;
	/**
	 * 
	 * @param conf
	 */
	protected FromFileVectorizer(LDARecommenderConfig conf, String itemid) {

		DocumentFilesPath = new Path(conf.getNewDocPath() + itemid);
		sequenceFilesPath = new Path(conf.getInferencePath() + "seq");
		sparseVectorOut= new Path(conf.getInferencePath() + "sparsein");	
		

	}

	protected void doTrain() throws Exception {
		generateSequneceFiles();
		generateSparseVectors(true,true,3,sequenceFilesPath,sparseVectorOut);

	}
	/**
	 * Generates SequenceFile
	 * @throws Exception 
	 */
	private void generateSequneceFiles() throws Exception{
		List<String> argList = Lists.newLinkedList();
        argList.add("-i");
        argList.add(DocumentFilesPath.toString());
        argList.add("-o");
        argList.add(sequenceFilesPath.toString());
        argList.add("-ow");
        String[] args = argList.toArray(new String[argList.size()]);
        ToolRunner.run(new SequenceFilesFromDirectory(), args);

	}
	/**
	 * 
	 * @param tfWeighting, either if true tf(unnormalized term-frequency) else TFIDF(normalized through maxFrequncy)
	 * @param named, if true output Vectors are named
	 * @param maxDFSigma, Maximum Standard deviation of termfrequency, 
	 * @param inputPath
	 * @param outputPath
	 * @throws Exception
	 */
    private void generateSparseVectors (boolean tfWeighting,  boolean named, double maxDFSigma, Path inputPath, Path outputPath) throws Exception {
        List<String> argList = Lists.newLinkedList();
        argList.add("-i");
        argList.add(inputPath.toString());
        argList.add("-o");
        argList.add(outputPath.toString());
        argList.add("-seq");
        if (named) {
            argList.add("-nv");
        }
        if (maxDFSigma >= 0) {
            argList.add("--maxDFSigma");
            argList.add(String.valueOf(maxDFSigma));
        }
        if (tfWeighting) {
            argList.add("--weight");
            argList.add("tf");
        }else{
            argList.add("--weight");
            argList.add("tfidf");
        }
        String[] args = argList.toArray(new String[argList.size()]);
        //String[] seqToVectorArgs = {"--weight", "tfidf", "--input", inputPath.toString(), "--output",  outputPath.toString(), "--maxDFPercent", "70", "--maxNGramSize", "2", "--namedVector"};
        ToolRunner.run(new SparseVectorsFromSequenceFiles(), args);
    }
    
}
