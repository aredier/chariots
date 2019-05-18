<template>
  <v-app id="inspire" dark>
    <v-navigation-drawer
      temporary  
      v-model="drawer"
      clipped
      fixed
      app
    >
      <v-list dense>
        <v-list-tile  v-for="s in series.map(el => el.seriesName)"
          @click="selected_series = s">
          <v-list-tile-action>
            <v-icon >trending_up</v-icon>
          </v-list-tile-action>
          <v-list-tile-content>
            <v-list-tile-title>{{s}}</v-list-tile-title>
          </v-list-tile-content>
        </v-list-tile>
      </v-list>
    </v-navigation-drawer>
    <v-toolbar app fixed clipped-left>
      <v-toolbar-side-icon @click.stop="drawer = !drawer"></v-toolbar-side-icon>
      <v-toolbar-title>Chariots UI</v-toolbar-title>
      <v-spacer></v-spacer>
    </v-toolbar>
    <v-content>
        <data-page v-if="this.selected_series" :series_name="this.selected_series"></data-page>
    </v-content>
    <v-footer app fixed>
      <span>&copy; 2017</span>
    </v-footer>
  </v-app>
</template>

<script>

import HelloWorld from './components/HelloWorld';
import DataPage from "./views/DataPage"
import {mapGetters, mapActions} from 'vuex';

import gql from 'graphql-tag'

export default {
  name: 'App',
  components: {
    HelloWorld,
    DataPage
  },
  computed:{
    seriesNames(series) {
      return series.map(el => el.seriesName)
    }
  },
  methods: {
  },
  apollo : {
    series : gql`query {
      series{
        seriesName
        }
      }`
  },
  data: () => ({
      drawer: null,
      selected_series: null,
    }),
  props: {
    source: String
  }
}
</script>
