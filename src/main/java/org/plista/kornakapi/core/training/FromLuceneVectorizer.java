package org.plista.kornakapi.core.training;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.text.LuceneStorageConfiguration;
import org.apache.mahout.text.SequenceFilesFromLuceneStorage;
import org.apache.mahout.utils.vectors.RowIdJob;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;
import org.plista.kornakapi.core.config.LDARecommenderConfig;
import org.plista.kornakapi.core.config.RecommenderConfig;

import com.google.common.collect.Lists;

public class FromLuceneVectorizer extends AbstractTrainer{
	
	private LuceneStorageConfiguration luceneStorageConf;
	private Path indexFilesPath;
	private Path sequenceFilesPath;
	private Path sparseVectorOut;
	private Path sparseVectorInputPath;
	/**
	 * 
	 * @param conf
	 */
	protected FromLuceneVectorizer(RecommenderConfig conf) {
		super(conf);
		indexFilesPath = new Path(((LDARecommenderConfig)conf).getLuceneIndexPath());
		ArrayList<Path> idxs = new ArrayList<Path>();
		idxs.add(indexFilesPath);
		sequenceFilesPath = new Path(((LDARecommenderConfig)conf).getVectorOutputPath());
		sparseVectorOut= new Path(((LDARecommenderConfig)conf).getSparseVectorOutputPath());	
		sparseVectorInputPath = new Path(((LDARecommenderConfig)conf).getCVBInputPath());
		Configuration config = new Configuration();				
	    //not sure what id, name, text should be, evl name~ title, text~text_full, id~itemid//
        luceneStorageConf = new LuceneStorageConfiguration(config, 
                Arrays.asList(indexFilesPath), sequenceFilesPath, "itemid",
                Arrays.asList("itemid","title"));

	}

	protected void doTrain() throws Exception {
		SequenceFilesFromLuceneStorage lucene2seq = new SequenceFilesFromLuceneStorage();
		lucene2seq.run(luceneStorageConf);	
		generateSparseVectors(true,true,3,sequenceFilesPath,sparseVectorOut);
		ensureIntegerKeys(sparseVectorOut.suffix("/tf-vectors/part-r-00000"),sparseVectorInputPath);

	}

	@Override
	protected void doTrain(File targetFile, DataModel inmemoryData,
			int numProcessors) throws IOException {
		// TODO Auto-generated method stub
		
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
    protected void generateSparseVectors (boolean tfWeighting,  boolean named, double maxDFSigma, Path inputPath, Path outputPath) throws Exception {
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
    
    protected void ensureIntegerKeys(Path inputPath, Path outputPath){
        List<String> argList = Lists.newLinkedList();
        argList.add("-i");
        argList.add(inputPath.toString());
        argList.add("-o");
        argList.add(outputPath.toString());
        String[] args = argList.toArray(new String[argList.size()]);
    	RowIdJob asd  = new RowIdJob();
    	try {
			asd.run(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

}


