import Vue from 'vue';
import Vuex from 'vuex';

Vue.use(Vuex);

export default new Vuex.Store({
  state: {
    series: [
      {
        "seriesName": "foo",
        "point": []
      }
    ],
    loaded: false

  },
  getters: {
    seriesName: state => state.series.map(el => el.seriesName),
  },
  mutations: {
    SetApplicationData(state, new_series) {
      state.series = new_series;
      state.loaded = true;
    },
    Loading(state) {
      state.loaded = false;
    }

  },
  actions: {
    async fetchData({state, commit}) {
      commit("Loading");
      let data = [
        {
          "seriesName": "perf",
          "point": []
        }
      ];
      commit("SetApplicationData", data);
    }

  },
});
