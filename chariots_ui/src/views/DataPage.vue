<template>
  <div>
    <v-container fluid>
      <v-layout align-space-around justify-center column>
        <v-flex>
          <v-card  class="pa-4 ma-5" style=" border-radius: 25px" v-if="this.seriesData">
            <v-layout align-center justify-space-around column fill-height>
              <v-flex ><h1 class="mb-4">{{this.series_name}}</h1></v-flex>
              <v-chart :options="option" style="width: 100%"/>
            </v-layout>
          </v-card>
          <v-card  class="pa-4 ma-5" style=" border-radius: 25px">
            <h1> Numerical data</h1>
            <v-data-table :headers="headers" :items="tablePoints"
            class="elevation-0">
              <template v-slot:items="props">
                <td class="text-xs-center" v-for="key in headers">
                  {{ props.item[key.value]}}
                </td>
              </template>
            </v-data-table>
          </v-card>
        </v-flex>
      </v-layout>
    </v-container>
  </div>
</template>
<script>
import ECharts from 'vue-echarts'
import 'echarts/lib/chart/line'
import 'echarts/lib/component/tooltip'
import 'echarts/lib/component/legend'
import 'echarts/lib/component/title'
import gql from 'graphql-tag'


export default {
  components: {
    'v-chart': ECharts
  },
  props: {
    series_name: null,

  },
  apollo : {
    series (){
      return {
        query: gql`query fetchSeries($series: String!) {
          series (seriesName: $series) {
            graphicalDisplay,
            numericalDisplay,
            points{
              time,
              opVersion,
              data
            }
          }
        }`,
        variables () {
          return {
            series: this.series_name,
          }
        },
      }
    },
  },
  data: () => ({
  }),
  computed : {
    seriesData(){
      if (! this.series){
        return null
      }
      let data = this.series[0]
      data.points = data.points.map(point => {
        point.data = JSON.parse(point.data)
        return point
      })
      return data
    },
    headers(){
      if (!this.seriesData){
        return null
      }
      let template = this.seriesData.points[0];
      let headerFields = Object.keys(template).filter(el => el != "data" && el[0] != "_")
      headerFields.push(...Object.keys(template["data"]))
      return headerFields.map(col => ({
        text: col,
        value: col,
        align: "center",
        smooth: true,
      }))
    },
    tablePoints() {
      if (!this.seriesData){
        return null
      }
      return this.seriesData.points.map(el => {
        let res = {};
        Object.keys(el).filter(key => key[0] != "_").map(key => {
          if (key != "data") {res[key] = el[key]}
          else {Object.keys(el[key]).map(dataKey => {
            res[dataKey] = el[key][dataKey]
          })}
        });
        return res
      })
    },
    graphSeries (){
      if (!this.seriesData){
        return null
      }
      let series = {}
      this.seriesData.points.map(point => {
        Object.keys(point.data).map(key => {
          if (!(key in series)){
            console.log(key)
            series[key] = []
          }
          series[key].push(point.data[key])
        })
      })
      return Object.keys(series).map(seriesName => ({
        name: seriesName,
        type:'line',
        data: series[seriesName],
        smooth: true,
        showSymbol: false,
        }))
    },
    option () {
      if (!this.seriesData){
        return {}
      }
      return {
        tooltip: {
          trigger: 'axis',

        },
        color: ['#bcd3bb', '#e88f70', '#edc1a5', '#9dc5c8', '#e1e8c8', '#7b7c68', '#e5b5b5', '#f0b489', '#928ea8', '#bda29a'],
        grid :{
          borderColor: "white"
        },
        legend: {},
        xAxis: {
          axisLine: {
            lineStyle: {
              color: '#ccc'
            }
          },
          data: this.seriesData.points.map(el => el.time),
          },
        yAxis: {
          axisLine: {
            lineStyle: {
              color: '#ccc'
            }
          },
        },
        series: this.graphSeries
      };
    }
  },
}
</script>

