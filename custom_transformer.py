

from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol,Param,Params,TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable  
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType
from pyspark.sql import types
from pyspark.sql import functions
import config

class EmojiExtractor(
        Transformer, HasInputCol, HasOutputCol,
        DefaultParamsReadable, DefaultParamsWritable):

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super().__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

        def find_all_emojis(text):
            if text is None:
                return []
            output = config.EMOJI_RE.findall(text)
            return output

        self.emoji_udf = udf(find_all_emojis, ArrayType(StringType()))

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    # Required in Spark >= 3.0
    def setInputCol(self, value):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    # Required in Spark >= 3.0
    def setOutputCol(self, value):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def _transform(self, dataset):
        
        out_col = self.getOutputCol()

        in_col = dataset[self.getInputCol()]

        return dataset.withColumn(out_col, self.emoji_udf(in_col))


class ProbabilityToLabel(
        Transformer, HasInputCol, HasOutputCol,
        DefaultParamsReadable, DefaultParamsWritable):

    negativeThreshold = Param(Params._dummy(), "negativeThreshold", "negativeThreshold",
                      typeConverter=TypeConverters.toFloat)

    positiveThreshold = Param(Params._dummy(), "positiveThreshold", "positiveThreshold",
                      typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, negativeThreshold = 0.5, positiveThreshold = 0.5):
        super().__init__()
        self.negativeThreshold = Param(self, "negativeThreshold", "")
        self.positiveThreshold = Param(self, "positiveThreshold", "")
        self._setDefault(negativeThreshold=0.5, positiveThreshold = 0.5)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

        def convert_to_label(prob_vector):

            first_val = float(prob_vector[1])

            return int(first_val > self.getOrDefault(self.negativeThreshold)) + int(first_val > self.getOrDefault(self.positiveThreshold))

        self.first_element_udf = udf(convert_to_label, IntegerType())

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, negativeThreshold = 0.5, positiveThreshold = 0.5):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    # Required in Spark >= 3.0
    def setInputCol(self, value):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        return self._set(inputCol=value)

    # Required in Spark >= 3.0
    def setOutputCol(self, value):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        return self._set(outputCol=value)

    def _transform(self, dataset):
        
        out_col = self.getOutputCol()

        in_col = dataset[self.getInputCol()]

        return dataset.withColumn(out_col, self.first_element_udf(in_col))